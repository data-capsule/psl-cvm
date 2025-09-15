use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use log::{info, warn};
use tokio::{sync::{mpsc::{unbounded_channel, UnboundedReceiver}, Mutex}, task::JoinSet};

use crate::{config::AtomicConfig, crypto::CachedBlock, rpc::SenderType, sequencer::auditor::{per_worker_auditor::PerWorkerAuditor, snapshot_store::SnapshotStore}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::block_sequencer::VectorClock};

mod snapshot_store;
mod per_worker_auditor;

pub struct Auditor {
    config: AtomicConfig,
    gc_vcs: HashMap<String, VectorClock>,
    snapshot_store: SnapshotStore,

    block_rx: Receiver<CachedBlock>,
    gc_rx: UnboundedReceiver<(String, VectorClock)>,
    gc_counter: usize,
    gc_timer: Arc<Pin<Box<ResettableTimer>>>,

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

        let snapshot_store = SnapshotStore::new(all_worker_names.clone().cloned().collect::<Vec<_>>());
        let per_worker_auditors = all_worker_names.clone().map(|name| {
            let (tx, rx) = make_channel(_chan_depth);
            per_worker_auditor_txs.push(tx);
            
            Arc::new(Mutex::new(
                PerWorkerAuditor::new(
                    config.clone(), name.clone(), rx,
                    gc_tx.clone(), snapshot_store.clone()
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
            per_worker_auditor_txs,
            per_worker_auditors,
            per_worker_auditor_handles: JoinSet::new(),
            log_timer,
            gc_counter: 0,
            gc_timer,
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
            for (worker_name, read_vc) in gc_rx_buffer {
                self.handle_gc(worker_name, read_vc).await;
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
            Some((worker_name, read_vc)) = self.gc_rx.recv() => {
                self.handle_gc(worker_name, read_vc).await;
            }
            _ = self.gc_timer.wait() => {
                self.do_gc().await;
            }
        }

        Ok(())
    }

    async fn log_stats(&mut self) {
        info!("GC VC: {}, Snapshot store size: {}", self.get_min_gc_vc(), self.snapshot_store.size());

        if self.snapshot_store.ghost_reborn_counter() > 0 {
            warn!("Ghost reborn counter: {}", self.snapshot_store.ghost_reborn_counter());
        } else {
            info!("Ghost reborn counter: {}", self.snapshot_store.ghost_reborn_counter());
        }

        if self.snapshot_store.concurrent_repeated_work_counter() > 0 {
            warn!("Concurrent repeated work counter: {}", self.snapshot_store.concurrent_repeated_work_counter());
        } else {
            info!("Concurrent repeated work counter: {}", self.snapshot_store.concurrent_repeated_work_counter());
        }
    }

    async fn handle_block(&mut self, block: CachedBlock) {
        for tx in self.per_worker_auditor_txs.iter() {
            tx.send(block.clone()).await.unwrap();
        }
    }

    async fn handle_gc(&mut self, worker_name: String, read_vc: VectorClock) {
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

        self.snapshot_store.prune_lesser_snapshots(&min_vc).await;

        let all_worker_names = self.snapshot_store.store.iter().map(|mapref| mapref.key().clone()).collect::<Vec<_>>();

        // for worker_name in all_worker_names {
        //     self.snapshot_store.prune_concurrent_snapshots(&self.gc_vcs.iter()
        //             .filter(|(_worker, _)| worker_name != **_worker)
        //             .map(|(_worker, vc)| vc)
        //         .collect::<Vec<_>>(), &self.gc_vcs.values().collect::<Vec<_>>()).await;
        // }

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