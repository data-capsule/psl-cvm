use std::{collections::{HashMap, VecDeque}, sync::Arc};

use log::warn;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::checkpoint::ProtoBackfillNack, rpc::{client::{Client, PinnedClient}, SenderType}, utils::{channel::Receiver, StorageServiceConnector}};

pub struct LogServer {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    client: PinnedClient,
    storage: StorageServiceConnector,

    last_gced_n: HashMap<SenderType, u64>,
    gc_rx: Receiver<(SenderType, u64)>,

    fork_cache: HashMap<SenderType, VecDeque<CachedBlock>>,

    block_rx: Receiver<(SenderType, CachedBlock)>,
    query_rx: Receiver<ProtoBackfillNack>,


}

impl LogServer {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        storage: StorageServiceConnector,
        gc_rx: Receiver<(SenderType, u64)>,
        block_rx: Receiver<(SenderType, CachedBlock)>,
        query_rx: Receiver<ProtoBackfillNack>,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);

        Self {
            config,
            keystore,
            block_rx,
            query_rx,
            gc_rx,
            client: client.into(),
            storage,
            last_gced_n: HashMap::new(),
            fork_cache: HashMap::new(),
        }
    }

    pub async fn run(logserver: Arc<Mutex<LogServer>>) {
        let mut logserver = logserver.lock().await;

        while let Ok(_) = logserver.worker().await {

        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            sender_and_n = self.gc_rx.recv() => {
                if sender_and_n.is_none() {
                    return Err(());
                }
                let (sender, n) = sender_and_n.unwrap();

                self.handle_gc(sender, n).await;
            },
            sender_and_block = self.block_rx.recv() => {
                if sender_and_block.is_none() {
                    return Err(());
                }
                let (sender, block) = sender_and_block.unwrap();

                self.handle_block(block, sender).await;
            },
            query = self.query_rx.recv() => {
                if query.is_none() {
                    return Err(());
                }
                let query = query.unwrap();

                self.handle_query(query).await;
            }
        }

        Ok(())
    }

    async fn handle_block(&mut self, block: CachedBlock, sender: SenderType) {
        let fork = self.fork_cache.entry(sender.clone()).or_insert_with(VecDeque::new);

        if fork.len() > 0 {
            if fork.iter().last().unwrap().block.n + 1 != block.block.n {
                warn!("Dropping block, continuity broken. Expected n = {}, got {}", fork.iter().last().unwrap().block.n + 1, block.block.n);
                return;
            }
        } else {
            if block.block.n != 1 {
                warn!("Dropping block, continuity broken. Expected n = 1, got {}", block.block.n);
                return;
            }
        }

        fork.push_back(block);
    }

    async fn handle_gc(&mut self, sender: SenderType, n: u64) {
        if !self.fork_cache.contains_key(&sender) {
            return;
        }

        let fork = self.fork_cache.get_mut(&sender).unwrap();

        if fork.len() == 0 {
            return;
        }

        let last_gced_n = self.last_gced_n
            .entry(sender.clone())
            .or_insert(0);

        if *last_gced_n < n {
            *last_gced_n = n;
        }

        fork.retain(|b| b.block.n > n);
    }

    async fn handle_query(&mut self, query: ProtoBackfillNack) {
        // TODO: Change datatype for query.
    }
}