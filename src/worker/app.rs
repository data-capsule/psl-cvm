use std::{future::Future, sync::Arc};

use tokio::sync::Mutex;

use crate::{config::AtomicConfig, consensus::batch_proposal::TxWithAckChanTag};

use super::cache_manager::CacheCommand;

pub trait CacheConnector {
    fn dispatch_read_request(
        &self,
        command: CacheCommand,
    ) -> anyhow::Result<()>;

    fn dispatch_write_request(
        &self,
        command: CacheCommand,
    ) -> anyhow::Result<()>;
}

pub trait ClientHandler {
    fn handle_client_request(
        &self,
        request: TxWithAckChanTag,
    ) -> anyhow::Result<()>;
}

pub trait PSLAppEngine {
    type CacheConnector: CacheConnector;
    type ClientHandler: ClientHandler;

    fn new(config: AtomicConfig, cache_connector: Self::CacheConnector, client_handler: Self::ClientHandler) -> Self;
    fn run(app: Arc<Mutex<Self>>) -> impl Future<Output = anyhow::Result<()>> + Send;

}