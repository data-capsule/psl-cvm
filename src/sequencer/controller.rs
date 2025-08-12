use std::sync::Arc;

use log::info;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, rpc::client::PinnedClient, utils::channel::Receiver};

pub enum ControllerCommand {
    BlockWorkers,
    UnblockWorkers,
}

pub struct Controller {
    config: AtomicConfig,
    client: PinnedClient,
    command_rx: Receiver<ControllerCommand>,
}

impl Controller {
    pub fn new(config: AtomicConfig, client: PinnedClient, command_rx: Receiver<ControllerCommand>) -> Self {
        Self {
            config,
            client,
            command_rx,
        }
    }

    pub async fn run(controller: Arc<Mutex<Self>>) {
        let mut controller = controller.lock().await;
        
        while let Ok(()) = controller.worker().await {

        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let cmd = self.command_rx.recv().await.unwrap();

        match cmd {
            ControllerCommand::BlockWorkers => {
                self.block_workers().await;
            }
            ControllerCommand::UnblockWorkers => {
                self.unblock_workers().await;
            }
        }

        Ok(())
    }

    async fn block_workers(&mut self) {
        info!("Blocking workers.");
    }

    async fn unblock_workers(&mut self) {
        info!("Unblocking workers.");
    }
}