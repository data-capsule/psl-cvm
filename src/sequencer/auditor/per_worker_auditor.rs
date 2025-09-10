use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::{info, warn};
use tokio::sync::{mpsc::UnboundedSender, Mutex};

use crate::{config::AtomicConfig, crypto::CachedBlock, proto::consensus::ProtoBlock, rpc::SenderType, sequencer::auditor::snapshot_store::SnapshotStore, utils::{channel::Receiver, timer::ResettableTimer}, worker::{block_sequencer::VectorClock, cache_manager::{process_tx_op, CacheKey, CachedValue}}};

#[allow(dead_code)]
pub struct PerWorkerAuditor {
    config: AtomicConfig,
    worker_name: String,
    block_rx: Receiver<CachedBlock>,
    gc_tx: UnboundedSender<(String, VectorClock)>,
    snapshot_store: SnapshotStore,

    /// For each origin, all blocks committed and to be used for snapshot.
    update_buffer: HashMap<String /* origin */, VecDeque<CachedBlock>>,

    /// For each origin, highest block that has been committed.
    available_vc: VectorClock,

    /// For `self.worker_name`, all blocks that are not yet audited.
    unaudited_buffer: VecDeque<CachedBlock>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    /// This will be the scapegoat for GC.
    last_read_vc: VectorClock,

    __snapshot_reused_counter: usize,
    __snapshot_generated_counter: usize,
}

impl PerWorkerAuditor {
    pub fn new(config: AtomicConfig, worker_name: String, block_rx: Receiver<CachedBlock>, gc_tx: UnboundedSender<(String, VectorClock)>, snapshot_store: SnapshotStore) -> Self {
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
            last_read_vc: VectorClock::new(),

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
        let mut audit_successful = false;
        while !self.unaudited_buffer.is_empty() {
            // Doing things in a tight loop.
            // Let's yield to tokio here.
            tokio::task::yield_now().await;

            let read_vc = &self.unaudited_buffer.front().unwrap().clone()
                .block.vector_clock;
            let read_vc = VectorClock::from(read_vc.clone());

            if read_vc <= self.available_vc {
                let block = self.unaudited_buffer.pop_front().unwrap();
                self.do_audit_block(block, read_vc).await;
                audit_successful = true;
            } else {
                break;
            }
        }

        audit_successful
    }

    async fn do_audit_block(&mut self, _block: CachedBlock, read_vc: VectorClock) {
        if !self.snapshot_store.snapshot_exists(&read_vc) {
            // TODO: Actually generate the updates.
            let updates = self.generate_updates(read_vc.clone());
            warn!("Generated updates: {}", updates.len());
            // let updates = vec![];
            self.snapshot_store.install_snapshot(read_vc.clone(), self.worker_name.clone(), updates).await;
            self.__snapshot_generated_counter += 1;
        } else {
            self.__snapshot_reused_counter += 1;
        }

        // TODO: Actually verify the reads.

        // This is unbounded so as to not cause deadlock.

        if self.last_read_vc != VectorClock::new() {
            self.gc_tx.send((self.worker_name.clone(), self.last_read_vc.clone())).unwrap();
        }
        self.last_read_vc = read_vc.clone();

    }

    fn generate_updates(&mut self, read_vc: VectorClock) -> Vec<(CacheKey, CachedValue)> {
        let update_blocks = self.get_update_blocks(read_vc);

        let updates = update_blocks.iter()
            .map(|block| self.filter_write_ops(&block.block))
            .flatten()
            .collect();


        updates
    }


    fn get_update_blocks(&mut self, read_vc: VectorClock) -> Vec<CachedBlock> {
        let chosen_glb = self.last_read_vc.clone();
        assert!(chosen_glb <= read_vc);
        assert!(self.snapshot_store.snapshot_exists(&chosen_glb));

        let mut update_blocks = Vec::new();
        let mut expected_block_count = 0;

        self.update_buffer.iter_mut().for_each(|(worker, queue)| {
            let glb_idx = chosen_glb.get(&SenderType::Auth(worker.clone(), 0));
            let end_idx = read_vc.get(&SenderType::Auth(worker.clone(), 0));
            if end_idx == 0 {
                return;
            }

            expected_block_count += end_idx - glb_idx;

            assert!(glb_idx <= end_idx);

            if glb_idx == end_idx {
                // Nothing to do.
                return;
            }

            if !queue.is_empty() {
                let front_n = queue.front().as_ref().unwrap().block.n;
                let back_n = queue.back().as_ref().unwrap().block.n;
                assert!(front_n <= end_idx, "front_n: {} end_idx: {} last_read_vc: {} read_vc: {} glb: {} worker: {}", front_n, end_idx, self.last_read_vc, read_vc, chosen_glb, worker);
                assert!(back_n >= glb_idx + 1, "back_n: {} glb_idx: {} last_read_vc: {} read_vc: {} glb: {} worker: {}", back_n, glb_idx, self.last_read_vc, read_vc, chosen_glb, worker);
                assert!(glb_idx + 1 >= front_n, "glb_idx: {} front_n: {} last_read_vc: {} read_vc: {} glb: {} worker: {}", glb_idx, front_n, self.last_read_vc, read_vc, chosen_glb, worker);
            }

            while !queue.is_empty() {
                let block = queue.pop_front().unwrap();
                let _n = block.block.n;
                if _n <= glb_idx {
                    continue;
                }
                update_blocks.push(block);

                if _n == end_idx {
                    break;
                }
            }
        });

        assert!(update_blocks.len() == expected_block_count as usize, "update_blocks.len(): {} expected_block_count: {} read_vc: {} lub: {}", update_blocks.len(), expected_block_count, read_vc, chosen_glb);

        update_blocks
    }

    fn filter_write_ops(&self, block: &ProtoBlock) -> Vec<(CacheKey, CachedValue)> {
        let mut updates = Vec::new();
        for tx in &block.tx_list {
            if tx.on_crash_commit.is_none() {
                continue;
            }

            let ops = tx.on_crash_commit.as_ref().unwrap();
            for op in &ops.ops {
                let Some((key, cached_value)) = process_tx_op(op) else { continue };
                updates.push((key, cached_value));
            }
        }
        updates
    }
    
}