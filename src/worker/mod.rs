use std::{io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::Arc};

use app::PSLAppEngine;
use cache_manager::{CacheCommand, CacheManager};
use log::{debug, warn};
use prost::Message as _;
use tokio::{sync::Mutex, task::JoinSet};

use crate::{config::{AtomicConfig, Config}, consensus::{batch_proposal::TxWithAckChanTag, fork_receiver::ForkReceiver}, crypto::{AtomicKeyStore, CryptoService, KeyStore}, proto::{checkpoint::ProtoBackfillNack, client::ProtoClientRequest, consensus::{ProtoAppendEntries, ProtoVote}, rpc::ProtoPayload}, rpc::{server::{MsgAckChan, RespType, Server, ServerContextType}, MessageRef, SenderType}, storage_server::{logserver::LogServer, staging::{self, Staging}}, utils::{channel::{make_channel, Receiver, Sender}, RocksDBStorageEngine, StorageService, StorageServiceConnector}, worker::{app::ClientHandlerTask, block_broadcaster::BlockBroadcaster}};

mod cache_manager;
mod block_broadcaster;
mod block_sequencer;
pub mod app;


pub struct PSLWorkerServerContext {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    backfill_request_tx: Sender<ProtoBackfillNack>,
    fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
    client_request_tx: Sender<TxWithAckChanTag>,
    staging_tx: Sender<ProtoVote>,
}

#[derive(Clone)]
pub struct PinnedPSLWorkerServerContext(pub Arc<Pin<Box<PSLWorkerServerContext>>>);

impl PinnedPSLWorkerServerContext {
    pub fn new(
        config: AtomicConfig,
        keystore: AtomicKeyStore,
        backfill_request_tx: Sender<ProtoBackfillNack>,
        fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
        client_request_tx: Sender<TxWithAckChanTag>,
        staging_tx: Sender<ProtoVote>,
    ) -> Self {
        let context = PSLWorkerServerContext {
            config,
            keystore,
            backfill_request_tx,
            fork_receiver_tx,
            client_request_tx,
            staging_tx,
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
    
    async fn handle_rpc(&self, m: MessageRef<'_>, ack_chan: MsgAckChan) -> Result<RespType, std::io::Error> {
        let sender = match m.2 {
            crate::rpc::SenderType::Anon => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "unauthenticated message",
                )); // Anonymous replies shouldn't come here
            }
            _sender @ crate::rpc::SenderType::Auth(_, _) => _sender.clone()
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
                self.fork_receiver_tx.send((proto_append_entries, sender)).await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            },
            crate::proto::rpc::proto_payload::Message::BackfillNack(proto_backfill_nack) => {
                self.backfill_request_tx.send(proto_backfill_nack).await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            },
            crate::proto::rpc::proto_payload::Message::ClientRequest(client_request) => {
                let client_tag = client_request.client_tag;
                self.client_request_tx.send((client_request.tx, (ack_chan, client_tag, sender))).await
                    .expect("Channel send error");

                return Ok(RespType::Resp);
            },
            crate::proto::rpc::proto_payload::Message::Vote(vote) => {
                self.staging_tx.send(vote).await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            }
            _ => {

            }
        }



        Ok(RespType::NoResp)
    }
}



pub struct PSLWorker<E: ClientHandlerTask + Send + Sync + 'static> {
    config: AtomicConfig,
    keystore: AtomicKeyStore,

    server: Arc<Server<PinnedPSLWorkerServerContext>>,
    crypto: CryptoService,

    cache_manager: Arc<Mutex<CacheManager>>,
    logserver: Arc<Mutex<LogServer>>,
    fork_receiver: Arc<Mutex<ForkReceiver>>,

    block_broadcaster_to_storage: Arc<Mutex<BlockBroadcaster>>,
    block_broadcaster_to_other_workers: Arc<Mutex<BlockBroadcaster>>,
    staging: Arc<Mutex<Staging>>,

    app: Arc<Mutex<PSLAppEngine<E>>>,
}

impl<E: ClientHandlerTask + Send + Sync + 'static> PSLWorker<E> {
    pub fn new(config: Config) -> Self {
        let (client_request_tx, client_request_rx) = make_channel(config.rpc_config.channel_depth as usize);
        Self::mew(config, client_request_tx, client_request_rx)
    }
    
    /// mew() must be called from within a Tokio context with channel passed in.
    /// This is new()'s cat brother.
    ///
    ///  /\_/\
    /// ( o.o )
    ///  > ^ < 
    pub fn mew(config: Config, client_request_tx: Sender<TxWithAckChanTag>, client_request_rx: Receiver<TxWithAckChanTag>) -> Self {
        let _chan_depth = config.rpc_config.channel_depth as usize;
        let _num_crypto_tasks = config.consensus_config.num_crypto_workers;

        let key_store = KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
        );
        let config = AtomicConfig::new(config);
        let keystore = AtomicKeyStore::new(key_store);
        let mut crypto = CryptoService::new(_num_crypto_tasks, keystore.clone(), config.clone());
        crypto.run();
        
        
        let (fork_receiver_tx, fork_receiver_rx) = make_channel(_chan_depth as usize);
        let (backfill_request_tx, backfill_request_rx) = make_channel(_chan_depth as usize);
        let (block_tx, block_rx) = make_channel(_chan_depth as usize);
        let (command_tx, command_rx) = make_channel(_chan_depth as usize);
        let (staging_tx, staging_rx) = make_channel(_chan_depth as usize);
        
        let context = PinnedPSLWorkerServerContext::new(
            config.clone(), keystore.clone(),
            backfill_request_tx, fork_receiver_tx, client_request_tx.clone(), staging_tx
        );
        let server = Arc::new(Server::new_atomic(
            config.clone(),
            context,
            keystore.clone(),
        ));


        let cache_manager = Arc::new(Mutex::new(CacheManager::new(command_rx, block_rx)));
        let logserver = Arc::new(Mutex::new(LogServer::new(config.clone(), keystore.clone(), )));
        let fork_receiver = Arc::new(Mutex::new(ForkReceiver::new()));
        let staging = Arc::new(Mutex::new(Staging::new()));

        let block_broadcaster_to_storage = Arc::new(Mutex::new(BlockBroadcaster::new(config.clone(), client, crypto, BroadcastMode::Star(config.consensus_config.node_list[0].clone()), true, block_rx, staging_tx)));
        let block_broadcaster_to_other_workers = Arc::new(Mutex::new(BlockBroadcaster::new(config.clone(), client, crypto, BroadcastMode::Gossip(config.consensus_config.node_list.clone()), false, block_rx, staging_tx)));

        let app = Arc::new(Mutex::new(PSLAppEngine::<E>::new(config.clone(), )));
        
        Self {
            config,
            keystore,
            server,
            crypto,

            cache_manager,
            logserver,
            fork_receiver,
            block_broadcaster_to_storage,
            block_broadcaster_to_other_workers,
            staging,
            app,
        }
    }

    pub fn run(&mut self) -> JoinSet<()> {
        let mut handles = JoinSet::new();

        let server = self.server.clone();
        let cache_manager = self.cache_manager.clone();
        let logserver = self.logserver.clone();
        let fork_receiver = self.fork_receiver.clone();
        let staging = self.staging.clone();
        let block_broadcaster_to_storage = self.block_broadcaster_to_storage.clone();
        let block_broadcaster_to_other_workers = self.block_broadcaster_to_other_workers.clone();
        let app = self.app.clone();

        handles.spawn(async move {
            let _ = Server::<PinnedPSLWorkerServerContext>::run(server).await;
        });

        handles.spawn(async move {
            let _ = CacheManager::run(cache_manager).await;
        });
        handles.spawn(async move {
            let _ = LogServer::run(logserver).await;
        });
        handles.spawn(async move {
            let _ = ForkReceiver::run(fork_receiver).await;
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


        handles
    }
}