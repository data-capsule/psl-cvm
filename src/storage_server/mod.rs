mod fork_receiver;
mod staging;
mod logserver;

use std::{io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::Arc};

use log::{debug, warn};
use prost::Message as _;
use tokio::{sync::Mutex, task::JoinSet};

use crate::{config::{AtomicConfig, Config}, crypto::{AtomicKeyStore, CryptoService, KeyStore}, proto::{checkpoint::ProtoBackfillNack, consensus::ProtoAppendEntries, rpc::ProtoPayload}, rpc::{server::{MsgAckChan, RespType, Server, ServerContextType}, MessageRef, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, RocksDBStorageEngine, StorageService}};
use fork_receiver::ForkReceiver;
use staging::Staging;
use logserver::LogServer;

pub struct StorageServerContext {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
    backfill_request_tx: Sender<(ProtoBackfillNack, MsgAckChan)>,
}

#[derive(Clone)]
pub struct PinnedStorageServerContext(pub Arc<Pin<Box<StorageServerContext>>>);

impl PinnedStorageServerContext {
    pub fn new(
        config: AtomicConfig,
        keystore: AtomicKeyStore,
        fork_receiver_tx: Sender<(ProtoAppendEntries, SenderType)>,
        backfill_request_tx: Sender<(ProtoBackfillNack, MsgAckChan)>,
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

    async fn handle_rpc(&self, m: MessageRef<'_>, ack_chan: MsgAckChan) -> Result<RespType, Error> {
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
                self.backfill_request_tx.send((proto_backfill_nack, ack_chan)).await
                    .expect("Channel send error");
                return Ok(RespType::Resp);
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
        backfill_request_tx: Sender<(ProtoBackfillNack, MsgAckChan)>,
        backfill_request_rx: Receiver<(ProtoBackfillNack, MsgAckChan)>,
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
                StorageService::new(_db, _chan_depth)
            },
            crate::config::StorageConfig::FileStorage(_) => {
                panic!("File storage not supported!");
            },
        };

        let ctx = PinnedStorageServerContext::new(
            config.clone(),
            keystore.clone(),
            fork_receiver_tx,
            backfill_request_tx,
        );
        let server = Server::new_atomic(config.clone(), ctx, keystore.clone());

        let fork_receiver_crypto = crypto.get_connector();
        let fork_receiver_storage = storage.get_connector(crypto.get_connector());
        let (staging_tx, staging_rx) = make_channel(_chan_depth);
        let (logserver_tx, logserver_rx) = make_channel(_chan_depth);
        let (fork_receiver_cmd_tx, fork_receiver_cmd_rx) = tokio::sync::mpsc::unbounded_channel();

        let fork_receiver = ForkReceiver::new(config.clone(), keystore.clone(), fork_receiver_rx, fork_receiver_crypto, fork_receiver_storage, staging_tx, fork_receiver_cmd_rx);

        let staging = Staging::new(config.clone(), keystore.clone(), staging_rx, logserver_tx, fork_receiver_cmd_tx);

        let logserver = LogServer::new(config.clone(), keystore.clone(), logserver_rx, backfill_request_rx);



        Self {
            config,
            keystore,
            server: Arc::new(server),
            storage: Arc::new(Mutex::new(storage)),
            crypto,
            fork_receiver: Arc::new(Mutex::new(fork_receiver)),
            staging: Arc::new(Mutex::new(staging)),
            logserver: Arc::new(Mutex::new(logserver)),
        }

    }

    pub async fn run(&mut self) -> JoinSet<()> {
        let mut handles = JoinSet::new();

        let server = self.server.clone();
        let storage = self.storage.clone();
        let fork_receiver = self.fork_receiver.clone();
        let staging = self.staging.clone();
        let logserver = self.logserver.clone();

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


        handles
    }
}