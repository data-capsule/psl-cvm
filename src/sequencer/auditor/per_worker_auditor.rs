use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::info;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::CachedBlock, rpc::SenderType, sequencer::auditor::snapshot_store::SnapshotStore, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::block_sequencer::VectorClock};

pub struct PerWorkerAuditor {
    config: AtomicConfig,
    worker_name: String,
    block_rx: Receiver<CachedBlock>,
    gc_tx: Sender<(String, VectorClock)>,
    snapshot_store: SnapshotStore,

    /// For each origin, all blocks committed and to be used for snapshot.
    update_buffer: HashMap<String /* origin */, VecDeque<CachedBlock>>,

    /// For each origin, highest block that has been committed.
    available_vc: VectorClock,

    /// For `self.worker_name`, all blocks that are not yet audited.
    unaudited_buffer: VecDeque<CachedBlock>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    __snapshot_reused_counter: usize,
    __snapshot_generated_counter: usize,
}

impl PerWorkerAuditor {
    pub fn new(config: AtomicConfig, worker_name: String, block_rx: Receiver<CachedBlock>, gc_tx: Sender<(String, VectorClock)>, snapshot_store: SnapshotStore) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));

        let _config = config.get();

        let all_worker_names = _config.net_config.nodes.keys()
            .filter(|name| name.starts_with("node"));

        let update_buffer = all_worker_names.clone().map(|name| (name.clone(), VecDeque::new())).collect();
        let available_vc = VectorClock::from_iter(all_worker_names.map(|name| (SenderType::Auth(name.clone(), 0), 0)));

        Self {
            config,
            worker_name,
            block_rx,
            gc_tx,
            snapshot_store,
            update_buffer,
            available_vc,
            unaudited_buffer: VecDeque::new(),
            log_timer,

            __snapshot_reused_counter: 0,
            __snapshot_generated_counter: 0,
        }
    }

    pub async fn run(per_worker_auditor: Arc<Mutex<Self>>) {
        let mut per_worker_auditor = per_worker_auditor.lock().await;

        per_worker_auditor.log_timer.run().await;

        while let Ok(_) = per_worker_auditor.worker().await {
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            }
            Some(block) = self.block_rx.recv() => {
                self.handle_block(block).await;
            }
        }

        self.maybe_audit_blocks().await;

        Ok(())
    }

    async fn handle_block(&mut self, block: CachedBlock) {
        
        let origin = block.block.origin.clone();
        if origin == self.worker_name {
            self.unaudited_buffer.push_back(block.clone());
        }

        self.update_buffer.get_mut(&origin).unwrap().push_back(block.clone());
        self.available_vc.advance(SenderType::Auth(origin.clone(), 0), block.block.n);
    }

    async fn log_stats(&mut self) {
        info!(
            "Worker: {}, Unaudited buffer size: {}, Reused snapshot counter: {} Generated snapshot counter: {}",
            self.worker_name, self.unaudited_buffer.len(),
            self.__snapshot_reused_counter, self.__snapshot_generated_counter
        );  
    }


    /// Returns true if blocks were audited.
    async fn maybe_audit_blocks(&mut self) -> bool {
        if self.unaudited_buffer.is_empty() {
            return false;
        }

        let read_vc = &self.unaudited_buffer.front().unwrap().clone()
            .block.vector_clock;
        let read_vc = VectorClock::from(read_vc.clone());

        if read_vc <= self.available_vc {
            let block = self.unaudited_buffer.pop_front().unwrap();
            self.do_audit_block(block, read_vc).await;
            return true;
        }

        return false;
    }

    async fn do_audit_block(&mut self, block: CachedBlock, read_vc: VectorClock) {
        if !self.snapshot_store.snapshot_exists(&read_vc) {
            // TODO: Actually generate the updates.
            let _vec = vec![];
            self.snapshot_store.install_snapshot(read_vc.clone(), _vec);
            self.__snapshot_generated_counter += 1;
        } else {
            self.__snapshot_reused_counter += 1;
        }

        // TODO: Actually verify the reads.

        self.gc_tx.send((self.worker_name.clone(), read_vc.clone())).await.unwrap();

    }
    
}