use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::{info, warn};
use prost::Message as _;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock, HashType}, proto::{checkpoint::ProtoBackfillQuery, consensus::{HalfSerializedBlock, ProtoAppendEntries, ProtoFork}}, rpc::{client::{Client, PinnedClient}, MessageRef, SenderType}, utils::{channel::Receiver, timer::ResettableTimer, StorageServiceConnector}};

pub struct LogServer {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    client: PinnedClient,
    storage: StorageServiceConnector,

    last_gced_n: HashMap<SenderType, u64>,
    gc_rx: Receiver<(SenderType, u64)>,

    fork_cache: HashMap<SenderType, VecDeque<CachedBlock>>,
    total_blocks_stored_num: usize,
    total_blocks_stored_bytes: usize,

    block_rx: Receiver<(SenderType, CachedBlock)>,
    query_rx: Receiver<ProtoBackfillQuery>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    submitted_block_window: usize,
    submitted_bytes_window: usize,


}

impl LogServer {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        storage: StorageServiceConnector,
        gc_rx: Receiver<(SenderType, u64)>,
        block_rx: Receiver<(SenderType, CachedBlock)>,
        query_rx: Receiver<ProtoBackfillQuery>,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);
        let log_timer = ResettableTimer::new(
            Duration::from_millis(config.get().app_config.logger_stats_report_ms)
        );
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
            log_timer,
            submitted_block_window: 0,
            submitted_bytes_window: 0,
            total_blocks_stored_num: 0,
            total_blocks_stored_bytes: 0,
        }
    }

    pub async fn run(logserver: Arc<Mutex<LogServer>>) {
        let mut logserver = logserver.lock().await;

        logserver.log_timer.run().await;

        while let Ok(_) = logserver.worker().await {

        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            _tick = self.log_timer.wait() => {
                self.log_stats().await;
            }
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

                self.submitted_block_window += 1;
                self.submitted_bytes_window += block.block_ser.len();
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

        self.total_blocks_stored_num += 1;
        self.total_blocks_stored_bytes += block.block_ser.len();
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

    async fn handle_query(&mut self, query: ProtoBackfillQuery) {
        let origin = query.origin;
        if origin.is_none() {
            return;
        }
        let origin = origin.unwrap();
        let origin = SenderType::Auth(origin.name, origin.sub_id);

        if !self.fork_cache.contains_key(&origin) {
            return;
        }

        let fork = self.fork_cache.get_mut(&origin).unwrap();

        let start_index = query.start_index;
        let end_index = query.end_index;

        // Get all the blocks stored in memory.
        let cached_blocks = fork.iter()
            .filter(|b| b.block.n >= start_index && b.block.n <= end_index)
            .map(|b| b.clone())
            .collect::<Vec<_>>();

        let mut gced_blocks = self.get_gced_blocks(origin, start_index).await;
        gced_blocks.extend(cached_blocks);
        let full_chain = gced_blocks;

        let ae = ProtoAppendEntries {
            fork: Some(ProtoFork {
                serialized_blocks: full_chain.iter()
                .map(|b| HalfSerializedBlock {
                    n: b.block.n,
                    view: 0,
                    view_is_stable: true,
                    config_num: 1,
                    origin: b.block.origin.clone(),
                    chain_id: b.block.chain_id,
                    serialized_body: b.block_ser.clone(),
                })
                .collect::<Vec<_>>(),
            }),
            commit_index: 0,
            view: 0,
            view_is_stable: true,
            config_num: 1,
            is_backfill_response: true,
        };

        self.reply_ae(query.reply_name, ae).await;
    }

    async fn reply_ae(&mut self, reply_name: String, ae: ProtoAppendEntries) {
        let buf = ae.encode_to_vec();
        let sz = buf.len();
        let msg = MessageRef(&buf, sz, &SenderType::Anon);
        let _ = PinnedClient::send(&self.client, &reply_name, msg).await;
    }

    /// Get all blocks from storage, starting with `start_index` and ending with `last_gced_n`.
    async fn get_gced_blocks(&mut self, origin: SenderType, mut start_index: u64) -> Vec<CachedBlock> {
        if start_index == 0 {
            start_index = 1; // We don't have a block with n = 0. This can mess up the while loop.
        }

        // Find if we really need to fetch anything.
        let last_gced_n = *(self.last_gced_n.get(&origin).unwrap_or(&0));
        if start_index > last_gced_n {
            return Vec::new();
        }
        
        let mut blocks = Vec::new();

        // Invariant: if start_index <= last_gced_n,
        // then the fork_cache isn't empty.

        // Find the parent hash to fetch.
        let parent_hash = self.fork_cache.get(&origin).unwrap() // unwrap is safe because of the invariant above.
            .front().unwrap()
            .block.parent.clone() as HashType;

        let mut n = last_gced_n;
        let mut curr_hash = parent_hash;
        while n >= start_index {
            let block = self.storage.get_block(&curr_hash).await;
            if block.is_err() {
                panic!("Failed to get block from storage: {:?}", block.err());
            }
            let block = block.unwrap();

            if block.block.n != n {
                panic!("Block n mismatch. Expected {}, got {}", n, block.block.n);
            }

            n -= 1;
            curr_hash = block.block.parent.clone() as HashType;
            blocks.push(block);
        }


        blocks
    }


    async fn log_stats(&mut self) {
        let total_forks = self.fork_cache.len();
        let avg_fork_size = if total_forks == 0 {
            0f64
        } else {
            self.fork_cache.values().map(|v| v.len()).sum::<usize>() as f64 / total_forks as f64
        };

        let block_throughput = self.submitted_block_window as f64
            / self.log_timer.timeout.as_secs_f64();

        let bytes_throughput = self.submitted_bytes_window as f64
            / self.log_timer.timeout.as_secs_f64();

        info!("Total forks: {} Avg fork size: {} Approx Throughput: {} blocks/s or {} MiB/s", total_forks, avg_fork_size, block_throughput, bytes_throughput / (1024.0 * 1024.0));
        info!("Total blocks stored: {} blocks or {} MiB", self.total_blocks_stored_num, self.total_blocks_stored_bytes as f64 / (1024.0 * 1024.0));
        self.submitted_block_window = 0;
        self.submitted_bytes_window = 0;
    }
}