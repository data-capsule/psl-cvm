use std::{io::Error, sync::Arc};

use tokio::sync::{mpsc::UnboundedSender, oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, rpc::{client::{Client, PinnedClient}, SenderType}, utils::channel::{Receiver, Sender}};

use super::fork_receiver::ForkReceiverCommand;

pub struct Staging {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    client: PinnedClient,
    block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType)>,
    logserver_tx: Sender<CachedBlock>,
    fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
}

impl Staging {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType)>,
        logserver_tx: Sender<CachedBlock>,
        fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);

        Self {
            config,
            keystore,
            block_rx,
            logserver_tx,
            fork_receiver_cmd_tx,
            client: client.into(),
        }
    }

    pub async fn run(staging: Arc<Mutex<Staging>>) {
        let mut staging = staging.lock().await;

        while let Ok(_) = staging.worker().await {

        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        let block_and_sender = self.block_rx.recv().await;

        if block_and_sender.is_none() {
            return Err(());
        }

        let (block, sender) = block_and_sender.unwrap();

        let block = block.await;

        if block.is_err() {
            return Err(());
        }

        let block = block.unwrap();

        match block {
            Ok(block) => {
                self.handle_checked_block(block, sender).await;
            }
            Err(err) => {
                // Handle error

                self.handle_error(err, sender).await;
            }
        }


        Ok(())
    }

    /// 1. Confirm to fork receiver 
    /// 2. Send to logserver
    /// 3. Send vote to sender.
    async fn handle_checked_block(&mut self, block: CachedBlock, sender: SenderType) {
        
    }

    /// 1. Rollback anything that is not confirmed.
    /// 2. Send backfill Nack to sender
    async fn handle_error(&mut self, err: Error, sender: SenderType) {
    }
}