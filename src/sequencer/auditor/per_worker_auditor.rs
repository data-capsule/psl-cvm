use std::{collections::{HashMap, HashSet, VecDeque}, pin::Pin, sync::Arc, time::{Duration, Instant}};

use itertools::Itertools;
use log::{error, info, trace, warn};
use tokio::sync::{mpsc::{UnboundedReceiver, UnboundedSender}, Mutex};

use crate::{config::AtomicConfig, crypto::CachedBlock, proto::consensus::{ProtoBlock, ProtoReadSet, ProtoReadSetEntry, ProtoVectorClockEntry}, rpc::SenderType, sequencer::auditor::SnapshotStore, utils::{channel::Receiver, timer::ResettableTimer}, worker::{block_sequencer::{cached_value_to_val_hash, VectorClock}, cache_manager::process_tx_op}};
use crate::utils::types::{CacheKey, CachedValue};


#[allow(dead_code)]
pub struct PerWorkerAuditor {
    config: AtomicConfig,
    worker_name: String,
    cache: HashMap<CacheKey, CachedValue>,
    cache_last_touched: HashMap<CacheKey, VectorClock>,

    block_rx: Receiver<CachedBlock>,
    gc_tx: UnboundedSender<(String, VectorClock, Vec<(CacheKey, CachedValue)>)>,
    snapshot_update_buffer: HashMap<VectorClock, Vec<(CacheKey, CachedValue)>>,

    min_gc_rx: UnboundedReceiver<VectorClock>,
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

    __snapshot_generated_counter: usize,

    __snapshot_block_size_sum: usize,
    __snapshot_block_count: usize,
    __fetched_from_snapshot_store_count: usize,
    __fetched_from_cache_count: usize,

    num_correct_reads: usize,
    num_incorrect_reads: usize,

    __audit_block_count: usize,
    __audit_block_time_sum: Duration,
    __gen_snapshot_time_sum: Duration,
    __verify_reads_time_sum: Duration,
}

impl PerWorkerAuditor {
    pub fn new(config: AtomicConfig, worker_name: String, block_rx: Receiver<CachedBlock>, gc_tx: UnboundedSender<(String, VectorClock, Vec<(CacheKey, CachedValue)>)>, min_gc_rx: UnboundedReceiver<VectorClock>, snapshot_store: SnapshotStore) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));

        let _config = config.get();

        let all_worker_names = _config.net_config.nodes.keys()
            .filter(|name| name.starts_with("node"));

        let update_buffer = all_worker_names.clone().map(|name| (name.clone(), VecDeque::new())).collect();
        let available_vc = VectorClock::from_iter(all_worker_names.map(|name| (SenderType::Auth(name.clone(), 0), 0)));

        Self {
            config,
            worker_name,
            cache: HashMap::new(),
            cache_last_touched: HashMap::new(),
            block_rx,
            gc_tx,
            snapshot_update_buffer: HashMap::new(),
            min_gc_rx,
            snapshot_store,
            update_buffer,
            available_vc,
            unaudited_buffer: VecDeque::new(),
            log_timer,
            last_read_vc: VectorClock::new(),

            __snapshot_generated_counter: 0,

            num_correct_reads: 0,
            num_incorrect_reads: 0,

            __snapshot_block_size_sum: 0,
            __snapshot_block_count: 0,

            __audit_block_count: 0,
            __audit_block_time_sum: Duration::from_secs(0),
            __gen_snapshot_time_sum: Duration::from_secs(0),
            __verify_reads_time_sum: Duration::from_secs(0),

            __fetched_from_snapshot_store_count: 0,
            __fetched_from_cache_count: 0,
        }
    }

    pub async fn run(per_worker_auditor: Arc<Mutex<Self>>) {
        let mut per_worker_auditor = per_worker_auditor.lock().await;

        per_worker_auditor.log_timer.run().await;

        while let Ok(_) = per_worker_auditor.worker().await {
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let min_gc_rx_pending = self.min_gc_rx.len();
        if min_gc_rx_pending > 0 {
            let mut min_gc_rx_buffer = Vec::with_capacity(min_gc_rx_pending);
            self.min_gc_rx.recv_many(&mut min_gc_rx_buffer, min_gc_rx_pending).await;
            // Take the latest one. Rest you can safely ignore.
            let min_gc = min_gc_rx_buffer.last().unwrap();
            
            self.do_gc(min_gc).await;
            return Ok(());
        }

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
            "Worker: {}, Unaudited buffer size: {}, Generated snapshot counter: {}, Cache size: {} MiB, Fetched from cache count: {} Fetched from snapshot store count: {} Correct reads: {} Incorrect reads: {}, Avg update size: {}, Avg audit time: {} us, Avg gen snapshot time: {} us, Avg verify reads time: {} us",
            self.worker_name, self.unaudited_buffer.len(),
            self.__snapshot_generated_counter,
            self.size() as f64 / 1024.0 / 1024.0,
            self.__fetched_from_cache_count, self.__fetched_from_snapshot_store_count,
            self.num_correct_reads, self.num_incorrect_reads,
            self.__snapshot_block_size_sum as f64 / self.__snapshot_block_count as f64,
            if self.__audit_block_count > 0 {
                self.__audit_block_time_sum.div_f64(self.__audit_block_count as f64).as_micros()
            } else {
                0
            },
            if self.__audit_block_count > 0 {
                self.__gen_snapshot_time_sum.div_f64(self.__audit_block_count as f64).as_micros()
            } else {
                0
            },
            if self.__audit_block_count > 0 {
                self.__verify_reads_time_sum.div_f64(self.__audit_block_count as f64).as_micros()
            } else {
                0
            }
        );

        // self.__audit_block_time_sum = Duration::from_secs(0);
        // self.__gen_snapshot_time_sum = Duration::from_secs(0);
        // self.__verify_reads_time_sum = Duration::from_secs(0);
        // self.__audit_block_count = 0;
    }


    async fn do_gc(&mut self, min_gc: &VectorClock) {
        let mut to_remove = Vec::new();
        for (key, vc) in self.cache_last_touched.iter() {
            if vc < min_gc {
                to_remove.push(key.clone());
            }
        }
        for key in to_remove {
            self.cache.remove(&key);
            self.cache_last_touched.remove(&key);
        }
    
    }


    /// Returns true if blocks were audited.
    async fn maybe_audit_blocks(&mut self) -> bool {
        let mut audit_successful = false;
        while !self.unaudited_buffer.is_empty() {
            // Doing things in a tight loop.
            // Let's yield to tokio here.
            tokio::task::yield_now().await;

            let block_ref = self.unaudited_buffer.front().unwrap();

            let read_vc = &block_ref.block.vector_clock;
            let read_vc = VectorClock::from(read_vc.clone());

            let mut all_read_vcs = Vec::new();
            all_read_vcs.push(read_vc.clone());

            if let Some(read_set) = &block_ref.block.read_set {
                for entry in &read_set.entries {
                    let vc_delta = &entry.vc_delta;
                    if vc_delta.is_none() {
                        continue;
                    }

                    let mut _vc = read_vc.clone();

                    let vc_delta = vc_delta.as_ref().unwrap();
                    for ProtoVectorClockEntry { sender, seq_num } in vc_delta.entries.iter() {
                        _vc.advance(SenderType::Auth(sender.clone(), 0), *seq_num);
                    }


                    // Invariant expected: vc_deltas are in "increasing" order.
                    if all_read_vcs.last().unwrap() != &_vc {
                        all_read_vcs.push(_vc);
                    }
                }
            }

            if all_read_vcs.len() > 1 {
                trace!("All read vcs: {:?}", all_read_vcs.len());
            }

            let can_audit = all_read_vcs.iter().all(|read_vc| read_vc <= &self.available_vc);

            if can_audit {
                let block = self.unaudited_buffer.pop_front().unwrap();
                self.do_audit_block(block, all_read_vcs).await;
                audit_successful = true;
            } else {
                break;
            }
        }

        audit_successful
    }

    async fn do_audit_block(&mut self, block: CachedBlock, all_read_vcs: Vec<VectorClock>) {
        let start_time = Instant::now();
        for read_vc in all_read_vcs {
            let start_time = Instant::now();
            let updates = self.generate_updates(read_vc.clone()).await;
            self.snapshot_update_buffer.insert(read_vc.clone(), updates.clone());
            for (key, value) in updates {
                self.put(key, value, read_vc.clone());
            }
            self.__snapshot_generated_counter += 1;
            self.__gen_snapshot_time_sum += Instant::now() - start_time;
            
            let _verify_reads_start_time = Instant::now();
            
            let write_ops = self.filter_write_ops_into_vec(&block.block);
            
            trace!("Write ops time: {:?} len: {}", Instant::now() - _verify_reads_start_time, write_ops.len());
            
            match &block.block.read_set {
                Some(read_set) => {
                    self.verify_reads(read_set, &read_vc, &write_ops).await;
                    trace!("Verify reads time: {:?} len: {}", Instant::now() - _verify_reads_start_time, read_set.entries.len());
                    
                },
                None => {
                    // No reads to verify.
                },
            }
            self.__verify_reads_time_sum += Instant::now() - _verify_reads_start_time;
            // This is unbounded so as to not cause deadlock.
    
            if self.last_read_vc != VectorClock::new() {
                let updates = self.snapshot_update_buffer.remove(&self.last_read_vc).unwrap();
                self.gc_tx.send((self.worker_name.clone(), self.last_read_vc.clone(), updates)).unwrap();
            }
            self.last_read_vc = read_vc.clone();
        }





        let end_time = Instant::now();
        self.__audit_block_time_sum += end_time - start_time;
        self.__audit_block_count += 1;

    }

    /// Precondition: base_vc <= read_vc && base_vc is in the snapshot store or is VectorClock::new()
    async fn generate_updates(&mut self, read_vc: VectorClock) -> Vec<(CacheKey, CachedValue)> {
        let (update_blocks, base_vc) = self.get_update_blocks(read_vc.clone());
        self.__snapshot_block_size_sum += update_blocks.len();
        self.__snapshot_block_count += 1;

        let update_map = update_blocks.iter()
            .map(|block| self.filter_write_ops_into_hashmap(&block.block))
            .collect::<Vec<HashMap<CacheKey, CachedValue>>>();

        let mut updates = HashMap::new();
        for map in update_map {
            for (key, value) in map {
                let entry = updates.entry(key).or_insert(value.clone());
                    // .merge_cached(value);

                match entry {
                    CachedValue::DWW(dww_val) => {
                        dww_val.merge_cached(value.get_dww().unwrap().clone());
                    },
                    CachedValue::PNCounter(pn_counter_val) => {
                        pn_counter_val.merge(value.get_pn_counter().unwrap().clone());
                    }
                }
            }
        }

        if base_vc != VectorClock::new() {
            for (key, value) in updates.iter_mut() {
                // let base_value = self.snapshot_store.get(key, &base_vc).await;
                let base_value = self.get(key);
                if base_value.is_none() {
                    continue;
                }
                // let _ = value.merge_cached(base_value.unwrap());
                match value {
                    CachedValue::DWW(dww_val) => {
                        dww_val.merge_cached(base_value.unwrap().get_dww().unwrap().clone());
                    },
                    CachedValue::PNCounter(pn_counter_val) => {
                        pn_counter_val.merge(base_value.unwrap().get_pn_counter().unwrap().clone());
                    }
                }
            }
        }

        // trace!("Updates: {:#?}", updates.iter().map(|(key, value)| (String::from_utf8(key.clone()).unwrap(), hex::encode(value.val_hash.to_bytes_be().1), read_vc.clone())).collect::<Vec<_>>());


        updates.into_iter().collect()
    }

    fn get(&mut self, key: &CacheKey) -> Option<CachedValue> {
        if self.cache.contains_key(key) {
            self.__fetched_from_cache_count += 1;
            self.cache.get(key).cloned()
        } else {
            self.__fetched_from_snapshot_store_count += 1;
            self.snapshot_store.get(key)
        }
    }

    fn put(&mut self, key: CacheKey, value: CachedValue, vc: VectorClock) {
        self.cache.insert(key.clone(), value);
        self.cache_last_touched.insert(key, vc);
    }

    fn size(&self) -> usize {
        self.cache.iter().map(|(key, value)| key.len() + value.size()).sum()
    }


    fn get_update_blocks(&mut self, read_vc: VectorClock) -> (Vec<CachedBlock>, VectorClock) {
        let chosen_glb = self.last_read_vc.clone();
        // assert!(chosen_glb <= read_vc);
        // assert!(self.snapshot_store.snapshot_exists(&chosen_glb));

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

        (update_blocks, chosen_glb)
    }

    fn filter_write_ops_into_hashmap(&self, block: &ProtoBlock) -> HashMap<CacheKey, CachedValue> {
        let mut updates = HashMap::new();
        for tx in &block.tx_list {
            if tx.on_crash_commit.is_none() {
                continue;
            }

            let ops = tx.on_crash_commit.as_ref().unwrap();
            for op in &ops.ops {
                let Some((key, cached_value)) = process_tx_op(op) else { continue };
                let entry = updates.entry(key).or_insert(cached_value.clone());
                    // .merge_cached(cached_value);
                match entry {
                    CachedValue::DWW(dww_val) => {
                        dww_val.merge_cached(cached_value.get_dww().unwrap().clone());
                    },
                    CachedValue::PNCounter(pn_counter_val) => {
                        pn_counter_val.merge(cached_value.get_pn_counter().unwrap().clone());
                    }
                }
            }
        }
        updates
    }

    fn filter_write_ops_into_vec(&self, block: &ProtoBlock) -> Vec<(CacheKey, CachedValue)> {
        let mut updates = Vec::new();
        for tx in &block.tx_list {
            if tx.on_crash_commit.is_none() {
                continue;
            }

            let ops = tx.on_crash_commit.as_ref().unwrap();
            for op in &ops.ops {
                let Some((key, cached_value)) = process_tx_op(op) else { continue };
                let _ = updates.push((key, cached_value));
            }
        }
        updates
    }


    /// Precondition: The snapshot wrt read_vc already exists in the snapshot store and is not GCed.
    /// Precondition: The read_set is sorted by after_write_op_index.
    async fn verify_reads(&mut self, read_set: &ProtoReadSet, read_vc: &VectorClock, write_ops: &Vec<(CacheKey, CachedValue)>) {
        let mut local_cache = HashMap::new();
        let mut cached_upto = 0;

        for ProtoReadSetEntry { key, value_hash, after_write_op_index, vc_delta } in &read_set.entries {
            let mut _read_vc = read_vc.clone();
            if vc_delta.is_some() {
                let vc_delta = vc_delta.as_ref().unwrap();
                for ProtoVectorClockEntry { sender, seq_num } in vc_delta.entries.iter() {
                    _read_vc.advance(SenderType::Auth(sender.clone(), 0), *seq_num);
                }
            }

            if _read_vc != *read_vc {
                continue;
            }
            
            while cached_upto < *after_write_op_index {
                assert!(cached_upto < write_ops.len() as u64);
                let (key, value) = write_ops[cached_upto as usize].clone();
                let _snapshot_value = self.get(&key);
                if _snapshot_value.is_some() {
                    local_cache.insert(key.clone(), _snapshot_value.unwrap());
                    let entry= local_cache.get_mut(&key).unwrap(); // .merge_cached(value);
                    match entry {
                        CachedValue::DWW(dww_val) => {
                            let _ = dww_val.merge_cached(value.get_dww().unwrap().clone());
                        },
                        CachedValue::PNCounter(pn_counter_val) => {
                            pn_counter_val.merge(value.get_pn_counter().unwrap().clone());
                        }
                    }
                } else {
                    local_cache.insert(key, value);
                }
                cached_upto += 1;
            }
            let correct_value = if local_cache.contains_key(key) {
                local_cache.get(key).cloned()
            } else {
                let __correct_value = self.get(key);

                __correct_value
            };
            let correct_value_hash = cached_value_to_val_hash(correct_value.clone());

            let read_match = match correct_value {
                Some(CachedValue::PNCounter(_)) => {
                    let buf1 = value_hash.as_slice().try_into();
                    let buf2 = correct_value_hash.as_slice().try_into();

                    if buf1.is_err() || buf2.is_err() {
                        false
                    } else {
                        (f64::from_be_bytes(buf1.unwrap()) - f64::from_be_bytes(buf2.unwrap())).abs() < 1e-6
                    }
                },
                _ => {
                    *value_hash == correct_value_hash
                }
            };
            if !read_match {
                let key_str = String::from_utf8(key.clone()).unwrap_or(hex::encode(key));
                let correct_value_hex_str = &hex::encode(correct_value_hash);
                let value_hex_str = &hex::encode(value_hash);

                warn!("❌ Read verification failed in {} for key: {} correct_value_hash: {} value_hash: {} read_vc: {}",
                    self.worker_name, key_str, correct_value_hex_str, value_hex_str, read_vc);


                self.num_incorrect_reads += 1;
            } else {
                let key_str = String::from_utf8(key.clone()).unwrap_or(hex::encode(key));
                let correct_value_hex_str = &hex::encode(correct_value_hash);
                let value_hex_str = &hex::encode(value_hash);
                trace!("✅ Read verification passed for key: {} correct_value_hash: {} value_hash: {} read_vc: {}",
                    key_str, correct_value_hex_str, value_hex_str, read_vc);

                self.num_correct_reads += 1;
            }
        }
    }
    
}