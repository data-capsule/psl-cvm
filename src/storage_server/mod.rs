pub mod fork_receiver;
pub mod staging;
pub mod logserver;

use std::{io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::Arc};

use log::{debug, warn};
use prost::Message as _;
use tokio::{sync::{mpsc::unbounded_channel, Mutex}, task::JoinSet};

use crate::{config::{AtomicConfig, Config}, crypto::{AtomicKeyStore, CryptoService, KeyStore}, proto::{checkpoint::ProtoBackfillQuery, consensus::ProtoAppendEntries, rpc::ProtoPayload}, rpc::{client::Client, server::{MsgAckChan, RespType, Server, ServerContextType}, MessageRef, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, RocksDBStorageEngine, StorageService}, worker::block_broadcaster::BroadcasterConfig};
use fork_receiver::ForkReceiver;
use staging::Staging;
use logserver::LogServer;
use crate::worker::block_broadcaster::{BlockBroadcaster, BroadcastMode};

pub struct StorageServerContext {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
    backfill_request_tx: Sender<ProtoBackfillQuery>,
}

#[derive(Clone)]
pub struct PinnedStorageServerContext(pub Arc<Pin<Box<StorageServerContext>>>);

impl PinnedStorageServerContext {
    pub fn new(
        config: AtomicConfig,
        keystore: AtomicKeyStore,
        fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
        backfill_request_tx: Sender<ProtoBackfillQuery>,
    ) -> Self {
        let context = StorageServerContext {
            config,
            keystore,
            fork_receiver_tx,
            backfill_request_tx,
        };
        Self(Arc::new(Box::pin(context)))
    }
}

impl Deref for PinnedStorageServerContext {
    type Target = StorageServerContext;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl ServerContextType for PinnedStorageServerContext {
    fn get_server_keys(&self) -> std::sync::Arc<Box<crate::crypto::KeyStore>> {
        self.keystore.get()
    }

    async fn handle_rpc(&self, m: MessageRef<'_>, _ack_chan: MsgAckChan) -> Result<RespType, Error> {
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
            crate::proto::rpc::proto_payload::Message::BackfillQuery(proto_backfill_query) => {
                self.backfill_request_tx.send(proto_backfill_query).await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            },

            _ => {
                // Drop
            }
        }



        Ok(RespType::NoResp)
    }
}


pub struct StorageNode {
    config: AtomicConfig,
    keystore: AtomicKeyStore,

    server: Arc<Server<PinnedStorageServerContext>>,
    storage: Arc<Mutex<StorageService<RocksDBStorageEngine>>>,
    crypto: CryptoService,

    fork_receiver: Arc<Mutex<ForkReceiver>>,
    staging: Arc<Mutex<Staging>>,
    logserver: Arc<Mutex<LogServer>>,

    /// Forward to sequencer.
    block_broadcaster: Arc<Mutex<BlockBroadcaster>>,
}

impl StorageNode {
    pub fn new(config: Config) -> Self {
        let (fork_receiver_tx, fork_receiver_rx) = make_channel(config.rpc_config.channel_depth as usize);
        let (backfill_request_tx, backfill_request_rx) = make_channel(config.rpc_config.channel_depth as usize);
        Self::mew(config, fork_receiver_tx, fork_receiver_rx, backfill_request_tx, backfill_request_rx)
    }

    pub fn mew(
        config: Config,
        fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
        fork_receiver_rx: Receiver<(ProtoAppendEntries, SenderType)>,
        backfill_request_tx: Sender<ProtoBackfillQuery>,
        backfill_request_rx: Receiver<ProtoBackfillQuery>,
    ) -> Self {
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
        let storage_config = &config.get().consensus_config.log_storage_config;
        let storage = match storage_config {
            rocksdb_config @ crate::config::StorageConfig::RocksDB(_) => {
                let _db = RocksDBStorageEngine::new(rocksdb_config.clone());
                StorageService::new(config.clone(), _db, _chan_depth)
            },
            _ => {
                panic!("Anything other than RocksDB is not supported!");
            },
        };

        let ctx = PinnedStorageServerContext::new(
            config.clone(),
            keystore.clone(),
            fork_receiver_tx.clone(),
            backfill_request_tx.clone(),
        );
        let server = Server::new_atomic(config.clone(), ctx, keystore.clone());

        let fork_receiver_crypto = crypto.get_connector();
        let fork_receiver_storage = storage.get_connector(crypto.get_connector());
        let (staging_tx, staging_rx) = unbounded_channel();
        let (logserver_tx, logserver_rx) = make_channel(_chan_depth);
        let (fork_receiver_cmd_tx, fork_receiver_cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gc_tx, gc_rx) = make_channel(_chan_depth);
        let (block_broadcaster_tx, block_broadcaster_rx) = make_channel(_chan_depth);


        // let _staging_tx = staging_tx.clone();
        // let _fork_receiver_tx = fork_receiver_tx.clone();
        // let _backfill_request_tx = backfill_request_tx.clone();
        // let _logserver_tx = logserver_tx.clone();
        // let _gc_tx = gc_tx.clone();
        // let _fork_receiver_cmd_tx = fork_receiver_cmd_tx.clone();
        // let _block_broadcaster_tx = block_broadcaster_tx.clone();

        // tokio::spawn(async move {
        //     loop {
        //         sleep(Duration::from_secs(1)).await;
        //         info!("Staging tx pending: {}", _staging_tx.len());
        //         info!("Fork receiver tx pending: {}", _fork_receiver_tx.len());
        //         info!("Backfill request tx pending: {}", _backfill_request_tx.len());
        //         info!("Logserver tx pending: {}", _logserver_tx.len());
        //         info!("GC tx pending: {}", _gc_tx.len());
        //         // info!("Fork receiver cmd tx pending: {}", _fork_receiver_cmd_tx.);
        //         info!("Block broadcaster tx pending: {}", _block_broadcaster_tx.len());
        //     }
        // });
        

        let fork_receiver = ForkReceiver::new(config.clone(), keystore.clone(), true, fork_receiver_rx, fork_receiver_crypto, fork_receiver_storage, staging_tx, fork_receiver_cmd_rx);

        let staging = Staging::new(config.clone(), keystore.clone(), staging_rx, logserver_tx, Some(gc_tx), fork_receiver_cmd_tx, Some(block_broadcaster_tx), true);

        let logserver_storage = storage.get_connector(crypto.get_connector());
        let logserver = LogServer::new(config.clone(), keystore.clone(), logserver_storage, gc_rx, logserver_rx, backfill_request_rx, None);

        let block_broadcaster_client = Client::new_atomic(config.clone(), keystore.clone(), false, 0).into();

        let block_broadcaster = BlockBroadcaster::new(BroadcasterConfig::Config(config.clone()), block_broadcaster_client, BroadcastMode::SequencerStar, false, false, block_broadcaster_rx, None, None);


        Self {
            config,
            keystore,
            server: Arc::new(server),
            storage: Arc::new(Mutex::new(storage)),
            crypto,
            fork_receiver: Arc::new(Mutex::new(fork_receiver)),
            staging: Arc::new(Mutex::new(staging)),
            logserver: Arc::new(Mutex::new(logserver)),
            block_broadcaster: Arc::new(Mutex::new(block_broadcaster)),
        }

    }

    pub async fn run(&mut self) -> JoinSet<()> {
        let mut handles = JoinSet::new();

        let server = self.server.clone();
        let storage = self.storage.clone();
        let fork_receiver = self.fork_receiver.clone();
        let staging = self.staging.clone();
        let logserver = self.logserver.clone();
        let block_broadcaster = self.block_broadcaster.clone();

        handles.spawn(async move {
            let mut storage = storage.lock().await;
            storage.run().await;
        });

        handles.spawn(async move {
            let _ = Server::<PinnedStorageServerContext>::run(server).await;
        });

        handles.spawn(async move {
            ForkReceiver::run(fork_receiver).await;
        });

        handles.spawn(async move {
            Staging::run(staging).await;
        });
        handles.spawn(async move {
            LogServer::run(logserver).await;
        });

        handles.spawn(async move {
            BlockBroadcaster::run(block_broadcaster).await;
        });


        handles
    }
}