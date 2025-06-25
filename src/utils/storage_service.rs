use std::{io::Error, pin::Pin, sync::Arc, time::{Duration, Instant}};

use log::info;
use tokio::sync::oneshot;

use crate::{config::AtomicConfig, crypto::{CachedBlock, CryptoServiceConnector, HashType}, utils::BlackHoleStorageEngine};

use super::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer, StorageEngine};

enum StorageServiceCommand {
    Put(HashType /* key */, Vec<u8> /* val */, oneshot::Sender<Result<(), Error>>),
    Get(HashType /* key */, oneshot::Sender<Result<Vec<u8>, Error>>),

    PutNonBlocking(oneshot::Receiver<Result<CachedBlock, Error>>, oneshot::Sender<Result<CachedBlock, Error>>),
}
pub struct StorageService<S: StorageEngine> {
    db: S,

    cmd_rx: Receiver<StorageServiceCommand>,
    cmd_tx: Sender<StorageServiceCommand>,

    config: AtomicConfig,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    aggregate_storage_latency_window: Duration,
    aggregate_storage_latency_count: usize,
}


pub struct StorageServiceConnector {
    cmd_tx: Sender<StorageServiceCommand>,
    crypto: CryptoServiceConnector,
}


impl<S: StorageEngine> StorageService<S> {
    pub fn new(config: AtomicConfig, db: S, buffer_size: usize) -> Self {
        let (cmd_tx, cmd_rx) = make_channel(buffer_size);
        let log_timer = ResettableTimer::new(
            Duration::from_millis(config.get().app_config.logger_stats_report_ms)
        );
        
        Self {
            db, cmd_rx, cmd_tx,
            config,
            log_timer,
            aggregate_storage_latency_window: Duration::from_millis(0),
            aggregate_storage_latency_count: 0,
        }
    }

    pub fn get_connector(&self, crypto: CryptoServiceConnector) -> StorageServiceConnector {
        StorageServiceConnector {
            cmd_tx: self.cmd_tx.clone(),
            crypto
        }
    }

    pub async fn run(&mut self) {
        self.db.init();
        self.log_timer.run().await;
        while let Ok(_) = self.worker().await {

        }

        self.db.destroy();
    }

    async fn worker(&mut self) -> Result<(), ()>{
        tokio::select! {
            _ = self.log_timer.wait() => {
                let latency = self.aggregate_storage_latency_window.as_millis();
                let count = self.aggregate_storage_latency_count;

                self.aggregate_storage_latency_window = Duration::from_millis(0);
                self.aggregate_storage_latency_count = 0;

                let latency = if count > 0 {
                    latency as f64 / count as f64
                } else {
                    0f64
                };

                if self.db.id() != "blackhole".to_string() {
                    info!("Avg Put Latency: {} ms", latency);
                }
            }
            cmd = self.cmd_rx.recv() => {
                if let Some(cmd) = cmd {
                    self.handle_cmd(cmd).await;
                }
            }
        }
        Ok(())
    }

    async fn handle_cmd(&mut self, cmd: StorageServiceCommand) {
        match cmd {
            StorageServiceCommand::Put(key, val, ok_chan) => {
                #[cfg(feature = "storage")]
                {
                    let start = Instant::now();
                    let res = self.db.put_block(&val, &key);
                    self.aggregate_storage_latency_window += start.elapsed();
                    self.aggregate_storage_latency_count += 1;
                    let _ = ok_chan.send(res);
                }

                #[cfg(not(feature = "storage"))]
                let _ = ok_chan.send(Ok(()));
            },
            StorageServiceCommand::Get(key, val_chan) => {
                let res = self.db.get_block(&key);
                let _ = val_chan.send(res);
            },

            StorageServiceCommand::PutNonBlocking(block_rx, ack_tx) => {
                #[cfg(feature = "storage")]
                {
                    let block = block_rx.await.unwrap();
                    if block.is_err() {
                        let _ = ack_tx.send(Err(block.unwrap_err()));
                        return;
                    }

                    let block = block.unwrap();
                    let start = Instant::now();
                    let res = self.db.put_block(&block.block_ser, &block.block_hash);
                    self.aggregate_storage_latency_window += start.elapsed();
                    self.aggregate_storage_latency_count += 1;
                    if res.is_err() {
                        let _ = ack_tx.send(Err(res.unwrap_err()));
                        return;
                    }
                    let _ = ack_tx.send(Ok(block));

                }

                #[cfg(not(feature = "storage"))]
                {
                    let _ = ack_tx.send(Ok(()));
                }
            }
        }
    }
}

pub type StorageAck = Result<(), Error>;

impl StorageServiceConnector {
    pub async fn get_block(&mut self, block_hash: &HashType) -> Result<CachedBlock, Error> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(StorageServiceCommand::Get(block_hash.clone(), tx)).await.unwrap();

        // Can't trust Disk to not have changed.
        self.crypto.check_block(block_hash.clone(), rx).await
    }

    pub async fn put_block(&self, block: &CachedBlock) -> oneshot::Receiver<StorageAck> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(StorageServiceCommand::Put(block.block_hash.clone(), block.block_ser.clone(), tx)).await.unwrap();

        rx
    }

    pub async fn put_raw(&self, key: String, val: Vec<u8>) -> oneshot::Receiver<StorageAck> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(StorageServiceCommand::Put(key.into_bytes(), val, tx)).await.unwrap();

        rx
    }

    pub async fn get_raw(&self, key: String) -> Result<Vec<u8>, Error> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(StorageServiceCommand::Get(key.into_bytes(), tx)).await.unwrap();

        rx.await.unwrap()
    }

    pub async fn put_nonblocking(&self, block: oneshot::Receiver<Result<CachedBlock, Error>>) -> oneshot::Receiver<Result<CachedBlock, Error>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(StorageServiceCommand::PutNonBlocking(block, tx)).await.unwrap();

        rx
    }
}
