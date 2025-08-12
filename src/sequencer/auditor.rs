use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::{error, info};
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::CachedBlock, sequencer::{commit_buffer::BlockStats, controller::ControllerCommand}, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::VectorClock, cache_manager::{CacheKey, CachedValue}}};

pub struct Auditor {
    config: AtomicConfig,
    auditor_rx: Receiver<(BlockStats, CachedBlock)>,
    controller_tx: Sender<ControllerCommand>,

    unaudited_buffer: HashMap<
        String, // origin
        VecDeque<(BlockStats, CachedBlock)>
    >,
    unaudited_buffer_size: usize,

    state_snapshots: HashMap<
        CacheKey,
        VecDeque<(VectorClock, CachedValue)>
    >,

    snapshot_vcs: VecDeque<VectorClock>,
    audit_timer: Arc<Pin<Box<ResettableTimer>>>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    /// Everything strictly < forgotten_cut is forgotten. The forgotten_cut itself is not forgotten. Or anything that is concurrent with it.
    forgotten_cut: VectorClock,

    dry_run_mode: bool,
}

impl Auditor {
    pub fn new(config: AtomicConfig, auditor_rx: Receiver<(BlockStats, CachedBlock)>, controller_tx: Sender<ControllerCommand>, dry_run_mode: bool) -> Self {
        let audit_timer = ResettableTimer::new(Duration::from_millis(config.get().consensus_config.max_audit_delay_ms));
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));

        Self {
            config,
            auditor_rx,
            controller_tx,

            unaudited_buffer: HashMap::new(),
            unaudited_buffer_size: 0,

            state_snapshots: HashMap::new(),
            snapshot_vcs: VecDeque::new(),

            audit_timer,
            log_timer,

            dry_run_mode,

            forgotten_cut: VectorClock::new(),
        }
    }

    pub async fn run(auditor: Arc<Mutex<Self>>) {
        let mut auditor = auditor.lock().await;

        auditor.audit_timer.run().await;
        auditor.log_timer.run().await;

        while let Ok(force_audit) = auditor.handle_inputs().await {
            if force_audit || auditor.should_audit() {
                auditor.do_audit().await;
            }
        }
    }

    async fn handle_inputs(&mut self) -> Result<bool, ()> {
        tokio::select! {
            _ = self.audit_timer.wait() => {
                Ok(true)
            }
            _ = self.log_timer.wait() => {
                self.log_stats().await;
                Ok(false)
            }
            Some((block_stats, block)) = self.auditor_rx.recv() => {
                self.handle_block(block_stats, block).await;
                Ok(false)
            }
        }
    }

    fn should_audit(&self) -> bool {
        self.unaudited_buffer_size >= self.config.get().consensus_config.max_audit_buffer_size
    }

    /// Until the buffer is empty, do the following:
    /// 1. Validate the first block in each origin's buffer.
    /// 2. Apply the updates in the block.
    /// 3. If number of snapshots exceeds the limit, send command to block the workers.
    /// 4. Unblock when the workers are updated.
    async fn do_audit(&mut self) {
        while self.unaudited_buffer_size > 0 {
            let Some((target_origin, _)) = self.unaudited_buffer.iter()
                .filter(|(_, queue)| !queue.is_empty()
                    && self.is_snapshot_available(&queue.front().unwrap().0.read_vc))
                .next()
            else {
                self.throw_error("There must have been a block that could have been audited. Invariant violated.");
                return;
            };

            let target_origin = target_origin.clone();

            let (block_stats, block) = self.unaudited_buffer.get_mut(&target_origin).unwrap()
                .pop_front().unwrap();

            self.unaudited_buffer_size -= 1;

            self.verify_reads(&block_stats, &block).await;

            self.apply_updates(&block_stats, &block).await;

            self.check_snapshot_limit().await;
        }
    }

    async fn handle_block(&mut self, block_stats: BlockStats, block: CachedBlock) {
        self.unaudited_buffer
            .entry(block_stats.origin.clone())
            .or_insert(VecDeque::new())
            .push_back((block_stats, block));
        self.unaudited_buffer_size += 1;
    }

    /// If my read_vc is X, what value must I have read?
    /// Invariant: All updates EXACTLY upto read_vc must be applied.
    /// Otherwise, the result could be wrong. This function is not going to check that.
    fn get_key(&self, key: &CacheKey, read_vc: &VectorClock) -> Option<CachedValue> {
        match self.state_snapshots.get(key) {
            Some(snapshots) => {
                if snapshots.is_empty() {
                    return None;
                }

                // Find all the vcs that are <= read_vc.
                // Then merge all the values.
                let value = snapshots.iter()
                    .filter(|(vc, _)| vc <= read_vc)
                    .map(|(_, value)| value.clone())
                    .reduce(|a, b| a.merge_immutable(&b))
                    .unwrap();

                Some(value)

            }
            None => None
        }
    }

    fn is_snapshot_available(&self, vc: &VectorClock) -> bool {
        // The zero vc is always available.
        if vc.iter().all(|(_, v)| *v == 0) {
            return true;
        }

        // Find an exact match for vc.
        self.snapshot_vcs.iter().find(|v| *v == vc).is_some()
    }

    async fn verify_reads(&self, block_stats: &BlockStats, block: &CachedBlock) {
        if self.dry_run_mode {
            return;
        }

        // TODO: Read Logging.

    }

    async fn apply_updates(&mut self, block_stats: &BlockStats, block: &CachedBlock) {
        
        self.snapshot_vcs.push_back(block_stats.read_vc.clone());
        
        if self.dry_run_mode {
            return;
        }
        
        // TODO: Replay tx list
    }

    async fn check_snapshot_limit(&mut self) {
        let mut frontier_cut = HashMap::new();
        for (_, queue) in &self.unaudited_buffer {
            if queue.is_empty() {
                continue;
            }

            let (block_stats, _) = queue.front().unwrap();

            for (origin, seq_num) in block_stats.read_vc.iter() {
                let _seq_num_curr = frontier_cut.entry(origin.clone()).or_insert(u64::MAX);
                if *seq_num < *_seq_num_curr {
                    *_seq_num_curr = *seq_num;
                }
            }
        }

        let frontier_cut = VectorClock::from_iter(frontier_cut.iter()
            .map(|(origin, seq_num)| (origin.clone(), *seq_num)));
        

        self.forget_snapshots(&frontier_cut).await;

        if self.state_snapshots.len() >= self.config.get().consensus_config.max_audit_snapshots {
            self.send_blocking_command().await;
        } else {
            self.send_unblocking_command().await;
        }
    }

    async fn throw_error(&self, error: &str) {
        error!("{}", error);
        panic!("Killing the auditor.");
        // Haven't decided what to do when audit fails.
    }
    
    async fn send_blocking_command(&mut self) {
        self.controller_tx.send(ControllerCommand::BlockWorkers).await.unwrap();
    }

    async fn send_unblocking_command(&mut self) {
        self.controller_tx.send(ControllerCommand::UnblockWorkers).await.unwrap();
    }

    async fn forget_snapshots(&mut self, forgotten_cut: &VectorClock) {
        self.state_snapshots.iter_mut()
            .for_each(|(_, vals)| {
                vals.retain(|(vc, _)| vc >= forgotten_cut);
            });

        self.snapshot_vcs.retain(|vc| vc >= forgotten_cut);

        self.forgotten_cut = forgotten_cut.clone();
    }

    async fn log_stats(&mut self) {
        info!("Snapshot VC: {:?}", self.snapshot_vcs);
    }
}