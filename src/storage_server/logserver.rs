use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::{info, warn};
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::checkpoint::ProtoBackfillNack, rpc::{client::{Client, PinnedClient}, SenderType}, utils::{channel::Receiver, timer::ResettableTimer, StorageServiceConnector}};

pub struct LogServer {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    client: PinnedClient,
    // storage: StorageServiceConnector,

    last_gced_n: HashMap<SenderType, u64>,
    gc_rx: Receiver<(SenderType, u64)>,

    fork_cache: HashMap<SenderType, VecDeque<CachedBlock>>,

    block_rx: Receiver<(SenderType, CachedBlock)>,
    query_rx: Receiver<ProtoBackfillNack>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    submitted_block_window: usize,
    submitted_bytes_window: usize,


}

impl LogServer {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        // storage: StorageServiceConnector,
        gc_rx: Receiver<(SenderType, u64)>,
        block_rx: Receiver<(SenderType, CachedBlock)>,
        query_rx: Receiver<ProtoBackfillNack>,
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
            // storage,
            last_gced_n: HashMap::new(),
            fork_cache: HashMap::new(),
            log_timer,
            submitted_block_window: 0,
            submitted_bytes_window: 0,
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

        self.submitted_block_window = 0;
        self.submitted_bytes_window = 0;
    }
}