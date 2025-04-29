use std::io::Error;

use tokio::sync::oneshot;

use crate::crypto::{CachedBlock, CryptoServiceConnector, HashType};

use super::{channel::{make_channel, Receiver, Sender}, StorageEngine};

enum StorageServiceCommand {
    Put(HashType /* key */, Vec<u8> /* val */, oneshot::Sender<Result<(), Error>>),
    Get(HashType /* key */, oneshot::Sender<Result<Vec<u8>, Error>>),

    PutNonBlocking(oneshot::Receiver<Result<CachedBlock, Error>>, oneshot::Sender<Result<CachedBlock, Error>>),
}

pub struct StorageService<S: StorageEngine> {
    db: S,

    cmd_rx: Receiver<StorageServiceCommand>,
    cmd_tx: Sender<StorageServiceCommand>
}


pub struct StorageServiceConnector {
    cmd_tx: Sender<StorageServiceCommand>,
    crypto: CryptoServiceConnector,
}


impl<S: StorageEngine> StorageService<S> {
    pub fn new(db: S, buffer_size: usize) -> Self {
        let (cmd_tx, cmd_rx) = make_channel(buffer_size);
        Self { db, cmd_rx, cmd_tx }
    }

    pub fn get_connector(&self, crypto: CryptoServiceConnector) -> StorageServiceConnector {
        StorageServiceConnector {
            cmd_tx: self.cmd_tx.clone(),
            crypto
        }
    }

    pub async fn run(&mut self) {
        self.db.init();
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                StorageServiceCommand::Put(key, val, ok_chan) => {
                    #[cfg(feature = "storage")]
                    {
                        let res = self.db.put_block(&val, &key);
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
                            continue;
                        }

                        let block = block.unwrap();
                        let res = self.db.put_block(&block.block_ser, &block.block_hash);
                        if res.is_err() {
                            let _ = ack_tx.send(Err(res.unwrap_err()));
                            continue;
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
        self.db.destroy();
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
