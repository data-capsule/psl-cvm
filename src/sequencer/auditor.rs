use std::{cmp::Ordering, collections::{BinaryHeap, HashMap, VecDeque}, fs::File, pin::Pin, sync::Arc, time::Duration};

use hashbrown::HashSet;
use log::{debug, error, info, trace};
use tokio::sync::Mutex;
use std::io::Write;

use crate::{config::AtomicConfig, crypto::CachedBlock, rpc::SenderType, sequencer::{commit_buffer::BlockStats, controller::ControllerCommand}, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::VectorClock, cache_manager::{process_tx_op, CacheKey, CachedValue}}};

struct BlockWithSeqNum(
    i128 /* this is negative of seq_num, since we use a max heap as min heap */,
    BlockStats, CachedBlock
);

impl PartialEq for BlockWithSeqNum {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for BlockWithSeqNum {}

impl PartialOrd for BlockWithSeqNum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl Ord for BlockWithSeqNum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}



pub struct Auditor {
    config: AtomicConfig,
    auditor_rx: Receiver<(BlockStats, CachedBlock)>,
    controller_tx: Sender<ControllerCommand>,

    unaudited_buffer: HashMap<
        String, // origin
        BinaryHeap<BlockWithSeqNum>
    >,
    unaudited_buffer_size: usize,

    state_snapshots: HashMap<
        CacheKey,
        VecDeque<(VectorClock, CachedValue)>
    >,

    snapshot_vcs: VecDeque<VectorClock>,
    audit_timer: Arc<Pin<Box<ResettableTimer>>>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    frontier_cut: HashMap<String, VectorClock>,

    /// Everything strictly < forgotten_cut is forgotten. The forgotten_cut itself is not forgotten. Or anything that is concurrent with it.
    forgotten_cut: VectorClock,

    /// For each origin, the last seq_num that was applied.
    applied_cut: VectorClock,

    dry_run_mode: bool,

    snapshot_log_file: File,
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
            snapshot_vcs: VecDeque::from_iter(vec![VectorClock::new()]),

            audit_timer,
            log_timer,

            dry_run_mode,

            forgotten_cut: VectorClock::new(),
            applied_cut: VectorClock::new(),
            frontier_cut: HashMap::new(),

            snapshot_log_file: File::create("snapshot.log").unwrap(),

        }
    }

    fn get_node_list(&self) -> Vec<String> {
        // There must be a better way to do this.
        self.config.get().net_config.nodes.iter()
            .filter(|(name, _)| name.starts_with("node"))
            .map(|(name, _)| name.clone())
            .collect()
    }
    pub async fn run(auditor: Arc<Mutex<Self>>) {
        let mut auditor = auditor.lock().await;

        auditor.audit_timer.run().await;
        auditor.log_timer.run().await;

        let node_list = auditor.get_node_list();
        auditor.frontier_cut
            .extend(node_list.iter().map(|name| (name.clone(), VectorClock::new())));


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
        self.cleanup_old_snapshots().await;

        while self.unaudited_buffer_size > 0 {
            let Some((target_origin, _)) = self.unaudited_buffer.iter()
                .filter(|(_, queue)| !queue.is_empty()
                    && self.is_snapshot_available(&queue.peek().unwrap().1.read_vc))
                .next()
            else {
                self.throw_error(format!("There must have been a block that could have been audited. Invariant violated. Applied cut: {}, Forgotten cut: {}", self.applied_cut, self.forgotten_cut).as_str()).await;
                return;
            };

            let target_origin = target_origin.clone();

            let BlockWithSeqNum(_, block_stats, block) = self.unaudited_buffer.get_mut(&target_origin).unwrap()
                .pop().unwrap();

            self.unaudited_buffer_size -= 1;

            self.verify_reads(&block_stats, &block).await;

            self.apply_updates(&block_stats, &block).await;

            self.update_frontier_cut().await;

            self.check_snapshot_limit().await;
        }
    }

    async fn handle_block(&mut self, block_stats: BlockStats, block: CachedBlock) {
        self.unaudited_buffer
            .entry(block_stats.origin.clone())
            .or_insert(BinaryHeap::new())
            .push(BlockWithSeqNum(-(block_stats.seq_num as i128), block_stats, block));
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
        debug!("Checking if snapshot is available for vc: {}.", vc);
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
        self.applied_cut.advance(SenderType::Auth(block_stats.origin.clone(), 0), block_stats.seq_num);
        
        // let mut block_vc = block_stats.read_vc.clone();
        // block_vc.advance(SenderType::Auth(block_stats.origin.clone(), 0), block_stats.seq_num);
        // self.snapshot_vcs.push_back(block_vc.clone());

        let vcs_to_apply = self.snapshot_vcs.iter()
            .filter(|vc| !(*vc < &block_stats.read_vc))
            .map(|vc| {
                let mut vc = vc.clone();
                vc.advance(SenderType::Auth(block_stats.origin.clone(), 0), block_stats.seq_num);
                vc
            })
            .collect::<HashSet<_>>();

        for vc in vcs_to_apply {
            self.snapshot_vcs.push_back(vc.clone());
            writeln!(self.snapshot_log_file, "{}", vc).unwrap();
        }

        self.snapshot_log_file.flush().unwrap();


        
        if self.dry_run_mode {
            return;
        }
        
        // for tx in &block.block.tx_list {
        //     let Some(on_crash_commit) = &tx.on_crash_commit else { continue };

        //     for op in &on_crash_commit.ops {
        //         let Some((key, cached_value)) = process_tx_op(op) else { continue };

        //         self.state_snapshots
        //             .entry(key.clone())
        //             .or_insert(VecDeque::new())
        //             .push_back((block_vc.clone(), cached_value));

        //     }
        // }

    }


    async fn cleanup_old_snapshots(&mut self) {
        // Remove all snapshots such that their vc is strictly less than all vcs in frontier_cut.
        self.snapshot_vcs.retain(|snapshot_vc| {
            let res = self.frontier_cut.iter()
                .all(|(_, read_vc)| {
                    debug!("Snapshot VC: {}, Read VC: {}. Result: {}", snapshot_vc, read_vc, *snapshot_vc < *read_vc);
                    *snapshot_vc < *read_vc
                });

            trace!("Snapshot VC: {}, Result: {}", snapshot_vc, res);

            !res
        });

        // Remove all snapshots that are concurrent with all vcs in frontier_cut.
        self.snapshot_vcs.retain(|snapshot_vc| {
            let res = self.frontier_cut.iter()
                .all(|(_, read_vc)| {
                    !(*snapshot_vc <= *read_vc || *read_vc <= *snapshot_vc)
                });

            !res
        });
    }

    async fn check_snapshot_limit(&mut self) {
        
        if self.state_snapshots.len() >= self.config.get().consensus_config.max_audit_snapshots {
            self.send_blocking_command().await;
        } else {
            self.send_unblocking_command().await;
        }
    }

    async fn throw_error(&self, error: &str) {
        error!("{}", error);
        // panic!("Killing the auditor.");
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
        info!("Number of snapshots: {}", self.snapshot_vcs.len());
        info!("Snapshot VCs: {:?}", self.snapshot_vcs);
        info!("Forgotten cut: {}", self.forgotten_cut);
        info!("Unaudited buffer size: {}", self.unaudited_buffer_size);
        info!("Frontier cut: {:?}", self.frontier_cut);
    }

    /// Frontier cut is the read_vc of the top of the heap for each origin.
    async fn update_frontier_cut(&mut self) {
        for (origin, queue) in &self.unaudited_buffer {
            if queue.is_empty() {
                continue;
            }

            let BlockWithSeqNum(_, block_stats, _) = queue.peek().unwrap();

            let frontier_cut = self.frontier_cut.entry(origin.clone()).or_insert(VectorClock::new());
            
            // Invariant: The read_vc must be >= the frontier cut.
            assert!(block_stats.read_vc >= *frontier_cut);
            
            if block_stats.read_vc > *frontier_cut {
                *frontier_cut = block_stats.read_vc.clone();
            }
        }
    }
}