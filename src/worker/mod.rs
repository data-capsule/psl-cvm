use std::{
    io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::Arc
};

use app::PSLAppEngine;
use cache_manager::CacheManager;
use log::{debug, warn};
use prost::Message as _;
use tokio::{
    sync::{mpsc::unbounded_channel, Mutex},
    task::JoinSet,
};

pub use crate::consensus::batch_proposal::TxWithAckChanTag;


#[cfg(feature = "external_cache")]
use crate::worker::external_cache_manager::ExternalCacheManager;
use crate::{
    config::{AtomicConfig, AtomicPSLWorkerConfig, PSLWorkerConfig},
    crypto::{AtomicKeyStore, CryptoService, KeyStore},
    proto::{
        checkpoint::ProtoBackfillQuery,
        consensus::ProtoAppendEntries,
        rpc::ProtoPayload,
    },
    rpc::{
        client::Client,
        server::{MsgAckChan, RespType, Server, ServerContextType},
        MessageRef, SenderType,
    },
    storage_server::logserver::LogServer,
    utils::{
        channel::{make_channel, Receiver, Sender},
        BlackHoleStorageEngine, RemoteStorageEngine, StorageService,
    },
    worker::{
        app::ClientHandlerTask,
        block_broadcaster::{BlockBroadcaster, BroadcastMode, BroadcasterConfig},
        block_sequencer::BlockSequencer,
        staging::VoteWithSender,
    },
};

pub mod app;
pub mod block_broadcaster;
pub mod block_sequencer;
pub mod cache_manager;
pub mod external_cache_manager;
pub mod staging;

use staging::Staging;

pub struct PSLWorkerServerContext {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    backfill_request_tx: Sender<ProtoBackfillQuery>,
    fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
    client_request_tx: Sender<TxWithAckChanTag>,
    sequencer_request_tx: Sender<TxWithAckChanTag>,
    staging_tx: Sender<VoteWithSender>,
}

#[derive(Clone)]
pub struct PinnedPSLWorkerServerContext(pub Arc<Pin<Box<PSLWorkerServerContext>>>);

impl PinnedPSLWorkerServerContext {
    pub fn new(
        config: AtomicConfig,
        keystore: AtomicKeyStore,
        backfill_request_tx: Sender<ProtoBackfillQuery>,
        fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
        client_request_tx: Sender<TxWithAckChanTag>,
        staging_tx: Sender<VoteWithSender>,
        sequencer_request_tx: Sender<TxWithAckChanTag>,
    ) -> Self {
        let context = PSLWorkerServerContext {
            config,
            keystore,
            backfill_request_tx,
            fork_receiver_tx,
            client_request_tx,
            staging_tx, 
            sequencer_request_tx,
        };
        Self(Arc::new(Box::pin(context)))
    }
}

impl Deref for PinnedPSLWorkerServerContext {
    type Target = PSLWorkerServerContext;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl ServerContextType for PinnedPSLWorkerServerContext {
    fn get_server_keys(&self) -> std::sync::Arc<Box<crate::crypto::KeyStore>> {
        self.keystore.get()
    }

    async fn handle_rpc(
        &self,
        m: MessageRef<'_>,
        ack_chan: MsgAckChan,
    ) -> Result<RespType, std::io::Error> {
        let sender = match m.2 {
            crate::rpc::SenderType::Anon => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "unauthenticated message",
                )); // Anonymous replies shouldn't come here
            }
            _sender @ crate::rpc::SenderType::Auth(_, _) => _sender.clone(),
        };
        let body = match ProtoPayload::decode(&m.0.as_slice()[0..m.1]) {
            Ok(b) => b,
            Err(e) => {
                warn!("Parsing problem: {} ... Dropping connection", e.to_string());
                debug!("Original message: {:?} {:?}", &m.0, &m.1);
                return Err(Error::new(ErrorKind::InvalidData, e));
            }
        };

        let msg = match body.message {
            Some(m) => m,
            None => {
                warn!("Nil message: {}", m.1);
                return Ok(RespType::NoResp);
            }
        };

        match msg {
            crate::proto::rpc::proto_payload::Message::AppendEntries(proto_append_entries) => {
                self.fork_receiver_tx
                    .send((proto_append_entries, sender))
                    .await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            }
            crate::proto::rpc::proto_payload::Message::BackfillQuery(proto_backfill_query) => {
                self.backfill_request_tx
                    .send(proto_backfill_query)
                    .await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            }
            crate::proto::rpc::proto_payload::Message::ClientRequest(client_request) => {
                // Is it the sequencer request?
                let (sender_name, _) = sender.to_name_and_sub_id();
                if sender_name.contains("sequencer") {
                    self.sequencer_request_tx
                        .send((client_request.tx, (ack_chan, client_request.client_tag, sender)))
                        .await
                        .expect("Channel send error");
                    return Ok(RespType::NoResp);
                }
                let client_tag = client_request.client_tag;
                
                self.client_request_tx
                    .send((client_request.tx, (ack_chan, client_tag, sender)))
                    .await
                    .expect("Channel send error");

                return Ok(RespType::Resp);
            }
            crate::proto::rpc::proto_payload::Message::Vote(vote) => {
                self.staging_tx
                    .send((sender, vote))
                    .await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            }
            _ => {}
        }

        Ok(RespType::NoResp)
    }
}

pub struct PSLWorker<E: ClientHandlerTask + Send + Sync + 'static> {
    config: AtomicPSLWorkerConfig,
    keystore: AtomicKeyStore,

    server: Arc<Server<PinnedPSLWorkerServerContext>>,
    crypto: CryptoService,

    #[cfg(feature = "external_cache")]
    cache_manager: Arc<Mutex<ExternalCacheManager>>,

    #[cfg(not(feature = "external_cache"))]
    cache_manager: Arc<Mutex<CacheManager>>,

    logserver: Arc<Mutex<LogServer>>,
    fork_receiver: Arc<Mutex<crate::storage_server::fork_receiver::ForkReceiver>>,
    block_sequencer: Arc<Mutex<BlockSequencer>>,
    block_broadcaster_to_storage: Arc<Mutex<BlockBroadcaster>>,
    block_broadcaster_to_other_workers: Arc<Mutex<BlockBroadcaster>>,
    staging: Arc<Mutex<Staging>>,
    storage: Arc<Mutex<StorageService<RemoteStorageEngine>>>,

    app: Arc<Mutex<PSLAppEngine<E>>>,
    __commit_rx_spawner: tokio::sync::broadcast::Receiver<u64>,
    __black_hole_storage: Arc<Mutex<StorageService<BlackHoleStorageEngine>>>,
}

impl<E: ClientHandlerTask + Send + Sync + 'static> PSLWorker<E> {
    pub fn new(config: PSLWorkerConfig) -> Self {
        let (client_request_tx, client_request_rx) =
            make_channel(config.rpc_config.channel_depth as usize);
        Self::mew(config, client_request_tx, client_request_rx)
    }

    /// mew() must be called from within a Tokio context with channel passed in.
    /// This is new()'s cat brother.
    /// ```
    ///  /\_/\
    /// ( o.o )
    ///  > ^ <
    /// ```
    pub fn mew(
        config: PSLWorkerConfig,
        client_request_tx: Sender<TxWithAckChanTag>,
        client_request_rx: Receiver<TxWithAckChanTag>,
    ) -> Self {
        let _chan_depth = config.rpc_config.channel_depth as usize;
        let _num_crypto_tasks = config.worker_config.num_crypto_workers;

        let key_store = KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
        );
        let config = AtomicPSLWorkerConfig::new(config);
        let keystore = AtomicKeyStore::new(key_store);

        let og_config = AtomicConfig::new(config.get().to_config());
        let mut crypto = CryptoService::new(_num_crypto_tasks, keystore.clone(), og_config.clone());
        crypto.run();

        // Wiring diagram:
        //                                   +-------------------------------------------------------------------------------------------------+
        //                                   |                                                                                                 |
        //                                   |                                                                                                 |
        //                                   |                                                                                                 |     +-----------+
        //                                   |                                                                                                 |     | LogServer |
        //                                   |                                                                                                 |     |           |
        //                                   |                                                                                                 |     +-----------+
        //                                   v                  +--------+                                                                     |           ^
        // +--------+                     +-------+             |        |               +-----------+           +------------------+     +----+----+      |
        // |        |                     |       |             |Cache   +-------------> |           |           | Block Broadcaster+---->| Staging |      |
        // | Client +-------------------->|  App  +------------>|Manager |               | Block     +---------->| to Storage       |     |         +------+---+
        // |        |                     |       |             |        |               | Sequencer |           +------------------+     +---------+ +--------v---------+
        // +--------+                     +-------+             +--------+               |           +----------------------------------------------->|Block Broadcaster |
        //                                                           ^                   +-----------+                                                |to other workers  |
        //                                 +-----------+             |                                                                                +------------------+
        //  +-------+                      |           |             |
        //  |Other  +--------------------->|  Fork     +-------------+
        //  |workers|                      |  Receiver |
        //  +-------+                      +-----------+

        let (backfill_request_tx, backfill_request_rx) = make_channel(_chan_depth as usize);
        let (fork_receiver_tx, fork_receiver_rx) = make_channel(_chan_depth as usize);
        let (vote_tx, vote_rx) = make_channel(_chan_depth as usize);
        let (cache_tx, cache_rx) = make_channel(_chan_depth as usize);
        let (block_tx, block_rx) = make_channel(_chan_depth as usize);
        let (command_tx, command_rx) = unbounded_channel();
        let (block_sequencer_tx, block_sequencer_rx) = make_channel(_chan_depth as usize);
        let (node_broadcaster_tx, node_broadcaster_rx) = make_channel(_chan_depth as usize);
        let (storage_broadcaster_tx, storage_broadcaster_rx) = make_channel(_chan_depth as usize);
        let (
            block_broadcaster_to_other_workers_deliver_tx,
            block_broadcaster_to_other_workers_deliver_rx,
        ) = make_channel(_chan_depth as usize);
        let (logserver_tx, logserver_rx) = make_channel(_chan_depth as usize);
        let (gc_tx, gc_rx) = make_channel(_chan_depth as usize);
        let (staging_tx, staging_rx) = make_channel(_chan_depth as usize);
        let (sequencer_request_tx, sequencer_request_rx) = make_channel(_chan_depth as usize);

        let context = PinnedPSLWorkerServerContext::new(
            og_config.clone(),
            keystore.clone(),
            backfill_request_tx,
            fork_receiver_tx,
            client_request_tx.clone(),
            vote_tx,
            sequencer_request_tx,
        );
        let server = Arc::new(Server::new_atomic(
            og_config.clone(),
            context,
            keystore.clone(),
        ));

        let (commit_tx_spawner, __commit_rx_spawner) =
            tokio::sync::broadcast::channel(_chan_depth as usize);

        let app = Arc::new(Mutex::new(PSLAppEngine::<E>::new(
            config.clone(),
            cache_tx,
            client_request_rx,
            commit_tx_spawner.clone(),
        )));

        let __black_hole_storage = StorageService::new(og_config.clone(), BlackHoleStorageEngine {}, _chan_depth);
        let fork_receiver = Arc::new(Mutex::new(
            crate::storage_server::fork_receiver::ForkReceiver::new(
                og_config.clone(),
                keystore.clone(),
                false,
                fork_receiver_rx,
                crypto.get_connector(),
                __black_hole_storage.get_connector(crypto.get_connector()),
                block_tx,
                command_rx,
            ),
        ));

        #[cfg(not(feature = "external_cache"))]
        let cache_manager = Arc::new(Mutex::new(CacheManager::new(
            config.clone(),
            cache_rx,
            sequencer_request_rx,
            block_rx,
            block_sequencer_tx,
            command_tx,
        )));

        #[cfg(feature = "external_cache")]
        let cache_manager = Arc::new(Mutex::new(ExternalCacheManager::new(
            config.clone(),
            keystore.clone(),
            cache_rx,
            block_rx,
            block_sequencer_tx,
            command_tx,
        )));

        let bs_client = Client::new_atomic(og_config.clone(), keystore.clone(), false, 0).into();
        let block_sequencer = Arc::new(Mutex::new(BlockSequencer::new(
            config.clone(),
            crypto.get_connector(),
            bs_client,
            block_sequencer_rx,
            node_broadcaster_tx,
            storage_broadcaster_tx,
            0, // Chain ID is set to 0 for PSL.
        )));

        let bb_ts_client = Client::new_atomic(og_config.clone(), keystore.clone(), false, 0).into();
        let block_broadcaster_to_storage = Arc::new(Mutex::new(BlockBroadcaster::new(
            BroadcasterConfig::WorkerConfig(config.clone()),
            bb_ts_client,
            BroadcastMode::StorageStar,
            true,
            false,
            storage_broadcaster_rx,
            None,
            Some(staging_tx),
        )));

        let staging = Arc::new(Mutex::new(Staging::new(
            config.clone(), 0,
            crypto.get_connector(),
            vote_rx,
            staging_rx,
            block_broadcaster_to_other_workers_deliver_tx,
            logserver_tx,
            commit_tx_spawner,
            gc_tx,
        )));

        let bb_ow_client = Client::new_atomic(og_config.clone(), keystore.clone(), false, 0).into();
        let block_broadcaster_to_other_workers = Arc::new(Mutex::new(BlockBroadcaster::new(
            BroadcasterConfig::WorkerConfig(config.clone()),
            bb_ow_client,
            BroadcastMode::WorkerGossip,
            false,
            true,
            node_broadcaster_rx,
            Some(block_broadcaster_to_other_workers_deliver_rx),
            None,
        )));

        let remote_storage = StorageService::<RemoteStorageEngine>::new(
            og_config.clone(),
            RemoteStorageEngine {
                config: og_config.clone(),
            },
            _chan_depth,
        );

        let logserver = Arc::new(Mutex::new(LogServer::new(
            og_config.clone(),
            keystore.clone(),
            remote_storage.get_connector(crypto.get_connector()),
            gc_rx,
            logserver_rx,
            backfill_request_rx,
            None,
        )));

        Self {
            config,
            keystore,
            server,
            crypto,

            cache_manager,
            logserver,
            fork_receiver,
            block_sequencer,
            block_broadcaster_to_storage,
            block_broadcaster_to_other_workers,
            staging,
            app,

            __commit_rx_spawner,
            __black_hole_storage: Arc::new(Mutex::new(__black_hole_storage)),
            storage: Arc::new(Mutex::new(remote_storage)),
        }
    }

    pub async fn run(&mut self) -> JoinSet<()> {
        let mut handles = JoinSet::new();

        let server = self.server.clone();
        let cache_manager = self.cache_manager.clone();
        let logserver = self.logserver.clone();
        let fork_receiver = self.fork_receiver.clone();
        let staging = self.staging.clone();
        let block_broadcaster_to_storage = self.block_broadcaster_to_storage.clone();
        let block_broadcaster_to_other_workers = self.block_broadcaster_to_other_workers.clone();
        let app = self.app.clone();
        let block_sequencer = self.block_sequencer.clone();

        let __black_hole_storage = self.__black_hole_storage.clone();
        let storage = self.storage.clone();

        handles.spawn(async move {
            let _ = Server::<PinnedPSLWorkerServerContext>::run(server).await;
        });

        handles.spawn(async move {
            #[cfg(not(feature = "external_cache"))]
            let _ = CacheManager::run(cache_manager).await;

            #[cfg(feature = "external_cache")]
            let _ = ExternalCacheManager::run(cache_manager).await;
        });

        handles.spawn(async move {
            let _ = BlockSequencer::run(block_sequencer).await;
        });

        handles.spawn(async move {
            let _ = LogServer::run(logserver).await;
        });
        handles.spawn(async move {
            let _ = crate::storage_server::fork_receiver::ForkReceiver::run(fork_receiver).await;
        });
        handles.spawn(async move {
            let _ = Staging::run(staging).await;
        });
        handles.spawn(async move {
            let _ = BlockBroadcaster::run(block_broadcaster_to_storage).await;
        });
        handles.spawn(async move {
            let _ = BlockBroadcaster::run(block_broadcaster_to_other_workers).await;
        });
        handles.spawn(async move {
            let _ = PSLAppEngine::run(app).await;
        });
        handles.spawn(async move {
            let mut __black_hole_storage = __black_hole_storage.lock().await;
            __black_hole_storage.run().await;
        });
        handles.spawn(async move {
            let mut storage = storage.lock().await;
            storage.run().await;
        });

        handles
    }
}
