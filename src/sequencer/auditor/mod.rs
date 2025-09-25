use std::{collections::HashMap, pin::Pin, sync::{atomic::fence, Arc}, time::Duration};

use dashmap::DashMap;
use log::{info, warn};
use tokio::{sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, Mutex}, task::JoinSet};

use crate::{config::AtomicConfig, crypto::CachedBlock, rpc::SenderType, sequencer::auditor::{per_worker_auditor::PerWorkerAuditor}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::VectorClock, cache_manager::{CacheKey, CachedValue}}};

mod per_worker_auditor;


#[derive(Clone)]
pub struct SnapshotStore(Arc<DashMap<CacheKey, CachedValue>>);

impl SnapshotStore {
    pub fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }

    pub fn get(&self, key: &CacheKey) -> Option<CachedValue> {
        self.0.get(key)
            .map(|mapref| mapref.value().clone())
    }

    pub fn insert(&self, key: CacheKey, value: CachedValue) {
        let mut entry = self.0.entry(key).or_insert(value.clone());
        let val = entry.value_mut();

        match val {
            CachedValue::DWW(dww_val) => {
                dww_val.merge_cached(value.get_dww().unwrap().clone());
            },
            CachedValue::PNCounter(pn_counter_val) => {
                pn_counter_val.merge(value.get_pn_counter().unwrap().clone());
            }
        }
    }

    pub fn size(&self) -> usize {
        self.0.iter().map(|mapref| mapref.key().len() + mapref.value().size()).sum()
    }
}

pub struct Auditor {
    config: AtomicConfig,
    gc_vcs: HashMap<String, VectorClock>,
    snapshot_store: SnapshotStore,

    block_rx: Receiver<CachedBlock>,
    gc_rx: UnboundedReceiver<(String, VectorClock, Vec<(CacheKey, CachedValue)>)>,
    per_worker_min_gc_txs: Vec<UnboundedSender<VectorClock>>,
    gc_counter: usize,
    gc_timer: Arc<Pin<Box<ResettableTimer>>>,
    snapshot_update_buffer: HashMap<VectorClock, Vec<(CacheKey, CachedValue)>>,

    per_worker_auditor_txs: Vec<Sender<CachedBlock>>,
    per_worker_auditors: Vec<Arc<Mutex<PerWorkerAuditor>>>,
    per_worker_auditor_handles: JoinSet<()>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl Auditor {
    pub fn new(config: AtomicConfig, block_rx: Receiver<CachedBlock>) -> Self {
        let _config = config.get();
        let _chan_depth = _config.rpc_config.channel_depth as usize;

        let all_worker_names = _config.net_config.nodes.keys()
            .filter(|name| name.starts_with("node"));

        let gc_vcs = all_worker_names.clone().map(|name| (name.clone(), VectorClock::new())).collect();
        let (gc_tx, gc_rx) = unbounded_channel();

        let mut per_worker_auditor_txs = Vec::new();
        let mut per_worker_min_gc_txs = Vec::new();

        let snapshot_store = SnapshotStore::new();
        let per_worker_auditors = all_worker_names.clone().map(|name| {
            let (tx, rx) = make_channel(_chan_depth);
            let (min_gc_tx, min_gc_rx) = unbounded_channel();
            per_worker_min_gc_txs.push(min_gc_tx);
            per_worker_auditor_txs.push(tx);
            
            Arc::new(Mutex::new(
                PerWorkerAuditor::new(
                    config.clone(), name.clone(), rx,
                    gc_tx.clone(), min_gc_rx, snapshot_store.clone()
                )
            ))
                
        }).collect();

        let log_timer = ResettableTimer::new(Duration::from_millis(_config.app_config.logger_stats_report_ms));
        let gc_timer = ResettableTimer::new(Duration::from_millis(_config.consensus_config.max_gc_interval_ms));

        Self {
            config,
            gc_vcs,
            gc_rx,
            snapshot_store,
            block_rx,
            per_worker_min_gc_txs,
            per_worker_auditor_txs,
            per_worker_auditors,
            per_worker_auditor_handles: JoinSet::new(),
            log_timer,
            gc_counter: 0,
            gc_timer,
            snapshot_update_buffer: HashMap::new(),
        }
    }

    pub async fn run(auditor: Arc<Mutex<Self>>) {
        let mut auditor = auditor.lock().await;

        let auditors = auditor.per_worker_auditors.drain(..).collect::<Vec<_>>();
        for per_worker_auditor in auditors {
            auditor.per_worker_auditor_handles.spawn(async move {
                PerWorkerAuditor::run(per_worker_auditor).await;
            });
        }

        auditor.log_timer.run().await;
        auditor.gc_timer.run().await;

        while let Ok(()) = auditor.worker().await {

        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let gc_rx_pending = self.gc_rx.len();
        if gc_rx_pending > 0 {
            let mut gc_rx_buffer = Vec::with_capacity(gc_rx_pending);
            self.gc_rx.recv_many(&mut gc_rx_buffer, gc_rx_pending).await;
            for (worker_name, read_vc, updates) in gc_rx_buffer {
                self.handle_gc(worker_name, read_vc, updates).await;
            }

            return Ok(());
        }
            
        tokio::select! {
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            }
            Some(block) = self.block_rx.recv() => {
                self.handle_block(block).await;
            }
            Some((worker_name, read_vc, updates)) = self.gc_rx.recv() => {
                self.handle_gc(worker_name, read_vc, updates).await;
            }
            _ = self.gc_timer.wait() => {
                self.do_gc().await;
            }
        }

        Ok(())
    }

    async fn log_stats(&mut self) {
        info!("GC VC: {}, Snapshot store size: {} MiB", self.get_min_gc_vc(), self.snapshot_store.size() as f64 / 1024.0 / 1024.0);
    }

    async fn handle_block(&mut self, block: CachedBlock) {
        for tx in self.per_worker_auditor_txs.iter() {
            tx.send(block.clone()).await.unwrap();
        }
    }

    async fn handle_gc(&mut self, worker_name: String, read_vc: VectorClock, updates: Vec<(CacheKey, CachedValue)>) {
        self.snapshot_update_buffer.insert(read_vc.clone(), updates);
        self.gc_vcs.insert(worker_name.clone(), read_vc);
        self.gc_counter += 1;

        let max_gc_counter = self.config.get().consensus_config.max_gc_counter;

        if self.gc_counter < max_gc_counter {
            return;
        }

        self.do_gc().await;
    }

    async fn do_gc(&mut self) {
        let min_vc = self.get_min_gc_vc();

        let mut to_remove = Vec::new();
        let mut updates = Vec::new();
        for (vc, _updates) in self.snapshot_update_buffer.iter() {
            if vc <= &min_vc {
                to_remove.push(vc.clone());
            }
        }
        for vc in to_remove {
            updates.extend(self.snapshot_update_buffer.remove(&vc).unwrap());
        }

        for (key, value) in updates {
            self.snapshot_store.insert(key, value);
        }

        // This ordering is very important.
        fence(std::sync::atomic::Ordering::Release);

        for tx in self.per_worker_min_gc_txs.iter() {
            tx.send(min_vc.clone()).unwrap();
        }

        self.gc_counter = 0;
        self.gc_timer.reset();
    }

    fn get_min_gc_vc(&self) -> VectorClock {
        let mut min_vc = HashMap::new();
        let all_worker_names = self.config.get().net_config.nodes.keys()
            .filter(|name| name.starts_with("node"))
            .map(|name| SenderType::Auth(name.clone(), 0))
            .collect::<Vec<_>>();

        for worker_name in &all_worker_names {
            min_vc.insert(worker_name.clone(), u64::MAX);
        }

        for (_, read_vc) in self.gc_vcs.iter() {
            for worker in &all_worker_names {
                let seq_num = read_vc.get(worker);
                if seq_num < *min_vc.get(worker).unwrap() {
                    min_vc.insert(worker.clone(), seq_num);
                }
            }
        }


        VectorClock::from_iter(min_vc.into_iter())
    }
    
    
}