use std::{cmp::Ordering, collections::{BinaryHeap, HashMap, HashSet, VecDeque}, fs::File, pin::Pin, sync::Arc, time::Duration};

use log::{debug, error, info, trace, warn};
use rand::seq::IteratorRandom;
use tokio::{sync::{oneshot, Mutex}, task::JoinSet};
use twox_hash::XxHash64;
use std::io::Write;

use crate::{config::AtomicConfig, crypto::CachedBlock, proto::consensus::ProtoVectorClock, rpc::SenderType, sequencer::{commit_buffer::BlockStats, controller::ControllerCommand}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::VectorClock, cache_manager::{process_tx_op, CacheKey, CachedValue}}};

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
    heartbeat_rx: Receiver<(ProtoVectorClock, SenderType)>,
    controller_tx: Sender<ControllerCommand>,

    unaudited_buffer: HashMap<
        String, // origin
        BinaryHeap<BlockWithSeqNum>
    >,
    unaudited_buffer_size: usize,

    snapshot_vcs: VecDeque<VectorClock>,
    update_buffer: HashMap<String /* origin */, VecDeque<CachedBlock>>,
    audit_timer: Arc<Pin<Box<ResettableTimer>>>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    frontier_cut: HashMap<String, VectorClock>,

    /// For each origin, all blocks that are committed.
    committed_vc: VectorClock,

    heartbeat_vcs: HashMap<SenderType, VectorClock>,

    dry_run_mode: bool,

    snapshot_log_file: File,
    shard_sender: ShardSender,

    shard_receivers: Vec<Receiver<ShardCommand>>,
    shard_handles: JoinSet<()>,
    block_processor_handles: JoinSet<()>,
    block_processor_tx: Sender<BlockProcessorCommand>,
    block_processor_rx: Receiver<BlockProcessorCommand>,
}

impl Auditor {
    const NUM_BLOCK_PROCESSORS: usize = 4;
    const NUM_SHARDS: usize = 16;


    pub fn new(config: AtomicConfig, auditor_rx: Receiver<(BlockStats, CachedBlock)>, controller_tx: Sender<ControllerCommand>, heartbeat_rx: Receiver<(ProtoVectorClock, SenderType)>, dry_run_mode: bool) -> Self {
        let audit_timer = ResettableTimer::new(Duration::from_millis(config.get().consensus_config.max_audit_delay_ms));
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        
        let _chan_depth = config.get().rpc_config.channel_depth as usize;
        let (shard_sender, shard_receivers) = ShardSender::new(Self::NUM_SHARDS, _chan_depth);
        
        let (block_processor_tx, block_processor_rx) = make_channel(_chan_depth);

        Self {
            config,
            auditor_rx,
            heartbeat_rx,
            controller_tx,
            heartbeat_vcs: HashMap::new(),

            unaudited_buffer: HashMap::new(),
            unaudited_buffer_size: 0,

            snapshot_vcs: VecDeque::from_iter(vec![VectorClock::new()]),
            update_buffer: HashMap::new(),

            audit_timer,
            log_timer,

            dry_run_mode,

            frontier_cut: HashMap::new(),
            committed_vc: VectorClock::new(),

            snapshot_log_file: File::create("snapshot.log").unwrap(),

            shard_sender,
            shard_receivers,
            shard_handles: JoinSet::new(),
            block_processor_handles: JoinSet::new(),
            block_processor_tx,
            block_processor_rx,
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

        for id in 0..Self::NUM_SHARDS {
            let shard_rx = auditor.shard_receivers.pop().unwrap();
            auditor.shard_handles.spawn(async move {
                ShardAuditor::new(id, shard_rx).run().await;
            });
        }

        for _ in 0..Self::NUM_BLOCK_PROCESSORS {
            let block_processor_rx = auditor.block_processor_rx.clone();
            let shard_sender = auditor.shard_sender.clone();
            auditor.block_processor_handles.spawn(async move {
                BlockProcessor::new(shard_sender, block_processor_rx).run().await;
            });
        }

        while let Ok(force_audit) = auditor.handle_inputs().await {
            if force_audit || auditor.should_audit() {
                auditor.do_audit().await;
            }
        }
    }

    async fn handle_inputs(&mut self) -> Result<bool, ()> {
        tokio::select! {
            biased;
            _ = self.log_timer.wait() => {
                self.log_stats().await;
                Ok(false)
            }
            Some((block_stats, block)) = self.auditor_rx.recv() => {
                self.handle_block(block_stats, block).await;
                Ok(false)
            }
            _ = self.audit_timer.wait() => {
                Ok(true)
            }
            Some((proto_heartbeat_vc, sender)) = self.heartbeat_rx.recv() => {
                debug!("Received heartbeat from worker: {:?} with vc: {:?}", sender, proto_heartbeat_vc);
                self.handle_heartbeat(proto_heartbeat_vc, sender).await;
                Ok(false)
            }
        }
    }

    async fn handle_heartbeat(&mut self, proto_heartbeat_vc: ProtoVectorClock, sender: SenderType) {
        self.heartbeat_vcs.insert(sender, VectorClock::from(Some(proto_heartbeat_vc)));
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
            let possible_targets = self.unaudited_buffer.iter()
                .filter_map(|(target, queue)| {
                    if queue.is_empty() {
                        return None;
                    }

                    let BlockWithSeqNum(_, block_stats, _) = queue.peek().unwrap();
                    let (cost, base_vc) = self.snapshot_gen_cost(&block_stats.read_vc);
                    Some((target.clone(), cost, base_vc))
                })
                .collect::<Vec<_>>();

            let min_cost = possible_targets.iter().min_by_key(|(_, cost, _)| *cost).unwrap().1;
            let (next_target, cost, base_vc) = possible_targets.iter()
                .filter(|(_, cost, _)| *cost == min_cost)
                .choose(&mut rand::thread_rng())
                .unwrap();

            if *cost == usize::MAX {
                let read_vcs = self.unaudited_buffer.iter().filter_map(|(origin, queue)| {
                    if queue.is_empty() {
                        return None;
                    }

                    let BlockWithSeqNum(_, block_stats, _) = queue.peek().unwrap();
                    Some((origin.clone(), block_stats.read_vc.clone()))
                }).collect::<HashMap<_, _>>();
                error!("Read VCs: {:?}, Snapshot GLB: {:?}", read_vcs, self.get_snapshot_vc_glb());

                self.throw_error("No snapshot can be generated for the read vc.").await;
                return;
            }

            trace!("Next target: {}, Cost: {}, Base VC: {}", next_target, cost, base_vc);



            let BlockWithSeqNum(_, block_stats, block) = self.unaudited_buffer.get_mut(next_target).unwrap()
                .pop().unwrap();

            self.unaudited_buffer_size -= 1;


            self.derive_snapshot(&block_stats.read_vc, &base_vc).await;

            self.verify_reads(&block_stats, &block).await;
            self.update_frontier_cut().await;

            self.apply_updates(&block_stats, &block).await;
            
        
        }

        self.cleanup_old_snapshots().await;
        self.cleanup_update_buffer().await;
        self.check_snapshot_limit().await;
    }

    async fn handle_block(&mut self, block_stats: BlockStats, block: CachedBlock) {
        self.committed_vc.advance(SenderType::Auth(block_stats.origin.clone(), 0), block_stats.seq_num);
        self.unaudited_buffer
            .entry(block_stats.origin.clone())
            .or_insert(BinaryHeap::new())
            .push(BlockWithSeqNum(-(block_stats.seq_num as i128), block_stats, block));
        self.unaudited_buffer_size += 1;
    }

    async fn verify_reads(&mut self, block_stats: &BlockStats, block: &CachedBlock) {
        if self.dry_run_mode {
            return;
        }

        // TODO: Read Logging.

    }

    async fn apply_updates(&mut self, block_stats: &BlockStats, block: &CachedBlock) {
        self.update_buffer.entry(block_stats.origin.clone())
            .or_insert(VecDeque::new())
            .push_back(block.clone());
    }


    async fn cleanup_old_snapshots(&mut self) {
        // Remove all snapshots such that their vc is strictly less than all vcs in frontier_cut.
        // self.snapshot_vcs.retain(|snapshot_vc| {
        //     let res = self.frontier_cut.iter()
        //         .all(|(_, read_vc)| {
        //             debug!("Snapshot VC: {}, Read VC: {}. Result: {}", snapshot_vc, read_vc, *snapshot_vc < *read_vc);
        //             *snapshot_vc < *read_vc
        //         });

        //     trace!("Snapshot VC: {}, Result: {}", snapshot_vc, res);

        //     !res
        // });

        // // Remove all snapshots that are concurrent with all vcs in frontier_cut.
        // self.snapshot_vcs.retain(|snapshot_vc| {
        //     let res = self.frontier_cut.iter()
        //         .all(|(_, read_vc)| {
        //             !(*snapshot_vc <= *read_vc || *read_vc <= *snapshot_vc)
        //         });

        //     !res
        // });

        self.snapshot_vcs.retain(|snapshot_vc| {
            self.frontier_cut.iter()
                .any(|(_, read_vc)| {
                    *snapshot_vc >= *read_vc
                })
        });

    }

    fn get_snapshot_lattice_diameter(&self) -> usize {
        Self::_get_snapshot_lattice_diameter(&self.heartbeat_vcs.values().cloned().collect::<HashSet<_>>())
    }


    /// Base cases:
    /// - If the list is empty, return 0.
    /// - If the list has only one element, return 1.
    /// Recursion:
    /// 1. Find the list of glbs.
    /// 2. For each glb:
    ///     - Find all vcs > glb.
    ///     - Call _get_snapshot_lattice_diameter on the list of vcs.
    /// 3. Add up the results.
    fn _get_snapshot_lattice_diameter(list: &HashSet<VectorClock>) -> usize {
        if list.is_empty() {
            return 0;
        }

        if list.len() == 1 {
            return 1;
        }

        let glbs = Self::_get_snapshot_vc_glb(list);
        error!("Glbs: {:?}", glbs);
        let mut diameter = 0;
        for glb in &glbs {
            let _diameter = Self::_get_snapshot_lattice_diameter(
                &list.iter()
                .filter_map(|vc| {
                    if vc > glb {
                        Some(vc.clone())
                    } else {
                        None
                    }
                }).collect());
            let _diameter = usize::max(1, _diameter);
            diameter += _diameter;
        }
        // error!("Glbs: {:?} Returning diameter: {}", glbs, diameter);

        diameter
    }



    async fn check_snapshot_limit(&mut self) {
        let diameter = self.get_snapshot_lattice_diameter();
        let blocking_criteria = diameter > self.config.get().consensus_config.max_audit_snapshots;
        let unique_heartbeat_vcs = self.heartbeat_vcs.values().collect::<HashSet<_>>();
        let unblocking_criteria = diameter <= self.config.get().consensus_config.max_audit_snapshots;


        warn!("Snapshot lattice diameter: {}", diameter);
        warn!("Unique heartbeat VCs: {:?}", unique_heartbeat_vcs);

        // Unblocking criteria takes precedence over blocking criteria.
        // Why? Because we may not have garbage collected enough old snapshots.
        // GC is triggered in do_audit, which in turn triggered if there are unaudited blocks.
        // If we make blocking the priority, we will never unblock.
        if unblocking_criteria {
            error!("Unblocking all workers.");
            self.send_unblocking_command().await;
        } else if blocking_criteria {
            error!("Blocking all workers.");
            self.send_blocking_command().await;
        }
    }

    async fn throw_error(&self, error: &str) {
        error!("{}", error);
        // panic!("Killing the auditor.");
        // Haven't decided what to do when audit fails.
    }
    
    async fn send_blocking_command(&mut self) {
        self.controller_tx.send(ControllerCommand::BlockAllWorkers).await.unwrap();
    }

    async fn send_unblocking_command(&mut self) {
        self.controller_tx.send(ControllerCommand::UnblockAllWorkers).await.unwrap();
    }

    fn get_snapshot_vc_glb(&self) -> Vec<VectorClock> {
        Self::_get_snapshot_vc_glb(&self.snapshot_vcs.iter().cloned().collect::<HashSet<_>>())
    }

    fn _get_snapshot_vc_glb(list: &HashSet<VectorClock>) -> Vec<VectorClock> {
        if list.len() <= 1 {
            return list.iter().map(|vc| vc.clone()).collect();
        }

        list.iter().filter(|test_vc| {
            list.iter().all(|other_vc| {
                !(other_vc < *test_vc)
            })
        })
        .map(|vc| vc.clone())
        .collect()
    }
    async fn log_stats(&mut self) {
        if self.snapshot_vcs.len() > self.config.get().consensus_config.max_audit_snapshots {
            warn!("Number of snapshots: {}", self.snapshot_vcs.len());
        } else {
            info!("Number of snapshots: {}", self.snapshot_vcs.len());
        }
        // info!("Snapshot VCs: {:?}", self.snapshot_vcs);
        info!("Snapshot VC GLB: {:?}", self.get_snapshot_vc_glb());
        info!("Unaudited buffer size: {}", self.unaudited_buffer_size);
        info!("Frontier cut: {:?}", self.frontier_cut);
        info!("Update buffer size: {}", self.update_buffer.iter().map(|(_, queue)| queue.len()).sum::<usize>());
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


    fn snapshot_gen_cost(&self, target_vc: &VectorClock) -> (usize, VectorClock) {
        // If any of the required blocks are not found in the update buffer, return MAX.
        if target_vc.iter()
            .any(|(sender, target_seq_num)| {
                let origin = sender.to_name_and_sub_id().0;
                let Some(buffer) = self.update_buffer.get(&origin)
                else {
                    return true;
                };

                buffer.back().map(|block| block.block.n).unwrap_or(0) < *target_seq_num
            }) {

            trace!("Target VC: {} not found in update buffer.", target_vc);
            return (usize::MAX, VectorClock::new());
        }

        self.snapshot_vcs.iter()
            .map(|vc| {
                if !(vc <= target_vc) {
                    return (usize::MAX, vc.clone());
                }

                let cost = target_vc.iter()
                    .map(|(sender, target_seq_num)| {
                        let seq_num = vc.get(sender);
                        target_seq_num - seq_num
                    })
                    .sum::<u64>();

                (cost as usize, vc.clone())
            })
            .min_by_key(|(cost, _)| *cost)
            .unwrap()
    
    }

    async fn derive_snapshot(&mut self, target_vc: &VectorClock, base_vc: &VectorClock) {
        if target_vc == base_vc {
            return;
        }

        self.snapshot_vcs.push_back(target_vc.clone());
        writeln!(self.snapshot_log_file, "{}", target_vc).unwrap();
        self.snapshot_log_file.flush().unwrap();

        if self.dry_run_mode {
            return;
        }

        let mut waiters = Vec::new();

        for (origin, end_seq_num) in target_vc.iter() {
            let base_seq_num = base_vc.get(origin);
            if base_seq_num == *end_seq_num {
                // Can't be >, since that will make base_vc and target_vc concurrent.
                continue;
            }
            let start_seq_num = base_seq_num + 1;

            // Find start_seq_num in the update buffer.
            let origin = origin.to_name_and_sub_id().0;
            let Some(buffer) = self.update_buffer.get(&origin)
            else {
                self.throw_error(&format!("No update buffer found for origin: {}.", origin)).await;
                return;
            };

            let Ok(index) = buffer.binary_search_by_key(&start_seq_num, |block| block.block.n)
            else {
                let range_start = buffer.front().map(|block| block.block.n).unwrap_or(0);
                let range_end = buffer.back().map(|block| block.block.n).unwrap_or(0);
                self.throw_error(&format!("Start seq_num: {} not found in update buffer for origin: {}. Current range: {}-{}", start_seq_num, origin, range_start, range_end)).await;
                return;
            };

            let block_cnt = (end_seq_num - start_seq_num + 1) as usize;

            for i in 0..block_cnt {
                let Some(block) = buffer.get(index + i)
                else {
                    self.throw_error(&format!("Block {} with supposed sequence number {} not found in update buffer for origin: {}.", index + i, start_seq_num + i as u64, origin)).await;
                    return;
                };

                // TODO: Apply the updates.
                info!("Applying update: {} for origin: {}.", block.block.n, origin);

                let (block_processor_command, block_processor_rx) = BlockProcessorCommand::new(block.clone(), target_vc.clone());

                self.block_processor_tx.send(block_processor_command).await.unwrap();

                waiters.push(block_processor_rx);

            }
        }

        for waiter in waiters.drain(..) {
            let _ = waiter.await;
        }

    }

    async fn cleanup_update_buffer(&mut self) {
        // Find min seq_num for each origin in each snapshot_vc.
        for (origin, queue) in self.update_buffer.iter_mut() {
            let _sender = SenderType::Auth(origin.clone(), 0);
            let min_seq_num = self.snapshot_vcs.iter()
                .map(|vc| {
                    vc.get(&_sender)
                })
                .min()
                .unwrap_or(0);

            while let Some(block) = queue.front() {
                if block.block.n < min_seq_num {
                    queue.pop_front();
                } else {
                    break;
                }
            }
        }
    }
}


enum ShardCommand {
    ApplyUpdates {
        snapshot_vc: VectorClock,
        updates: Vec<(CacheKey, CachedValue)>,
    },

    VerifyReads {
        snapshot_vc: VectorClock,
        reads: Vec<(CacheKey, CachedValue)>,

        response_tx: Option<oneshot::Sender<bool>>,
    },

    CleanSnapshots {
        snapshot_vc: VectorClock,
    }
}


struct SnapshotValue {
    /// Values appended here first.
    lazy_eval_log: VecDeque<(VectorClock, CachedValue)>,

    /// On the first read query for a given vc, we will evaluate the value from the lazy_eval_log.
    /// And add it here.
    absolute_value: HashMap<VectorClock, CachedValue>,

}

struct ShardAuditor {
    id: usize,

    state_snapshots: HashMap<
        CacheKey,
        SnapshotValue
    >,

    command_rx: Receiver<ShardCommand>,
}

impl ShardAuditor {
    pub fn new(id: usize, command_rx: Receiver<ShardCommand>) -> Self {
        Self {
            id,
            state_snapshots: HashMap::new(),
            command_rx,
        }
    }

    pub async fn run(&mut self) {
        let mut write_commands_executed = 0;
        let mut read_commands_executed = 0;

        while let Some(command) = self.command_rx.recv().await {
            match command {
                ShardCommand::ApplyUpdates { snapshot_vc, updates } => {
                    self.apply_updates(snapshot_vc, updates).await;
                    write_commands_executed += 1;
                },
                ShardCommand::VerifyReads { snapshot_vc, reads, mut response_tx } => {
                    let result = self.verify_reads(snapshot_vc, reads).await;
                    let response_tx = response_tx.take().unwrap();
                    let _ = response_tx.send(result);
                    read_commands_executed += 1;
                },
                ShardCommand::CleanSnapshots { snapshot_vc } => {
                    self.clean_snapshots(snapshot_vc).await;
                }
            }

            if write_commands_executed % 1000 == 1 || read_commands_executed % 1000 == 1 {
                info!("Shard {} Write commands executed: {}, Read commands executed: {}, Total keys: {}",
                    self.id, write_commands_executed, read_commands_executed, self.state_snapshots.len());
            }

            
        }
    }

    async fn apply_updates(&mut self, snapshot_vc: VectorClock, mut updates: Vec<(CacheKey, CachedValue)>) {
        for (key, value) in updates.drain(..) {
            self.state_snapshots.entry(key)
                .or_insert(SnapshotValue { lazy_eval_log: VecDeque::new(), absolute_value: HashMap::new() })
                .lazy_eval_log.push_back((snapshot_vc.clone(), value));
        }
    }

    async fn verify_reads(&mut self, snapshot_vc: VectorClock, reads: Vec<(CacheKey, CachedValue)>) -> bool {
        for (key, value) in reads {
            let Some(expected_value) = self.get_key(&key, &snapshot_vc)
            else {
                return false;
            };

            if expected_value != value {
                return false;
            }
        }
        true
    }

    async fn clean_snapshots(&mut self, snapshot_vc: VectorClock) {
        self.state_snapshots.iter_mut()
            .for_each(|(_, vals)| {
                vals.lazy_eval_log.retain(|(vc, _)| !(vc < &snapshot_vc));
                vals.absolute_value.retain(|vc, _| !(vc < &snapshot_vc));
            });
    }

    /// If my read_vc is X, what value must I have read?
    /// Invariant: All updates EXACTLY upto read_vc must be applied.
    /// Otherwise, the result could be wrong. This function is not going to check that.
    fn get_key(&mut self, key: &CacheKey, read_vc: &VectorClock) -> Option<CachedValue> {
        let Some(snapshots) = self.state_snapshots.get_mut(key)
        else {
            return None;
        };

        if snapshots.absolute_value.contains_key(read_vc) {
            return snapshots.absolute_value.get(read_vc).cloned();
        }

        let value = snapshots.lazy_eval_log.iter()
            .filter(|(vc, _)| vc <= read_vc)
            .map(|(_, value)| value.clone())
            .reduce(|a, b| a.merge_immutable(&b))
            .unwrap();

        snapshots.absolute_value.insert(read_vc.clone(), value.clone());

        Some(value)

    }
}


pub struct ShardSender {
    txs: Vec<Sender<ShardCommand>>,
}

impl ShardSender {
    const SEED: u64 = 42;

    pub fn new(num_shards: usize, chan_depth: usize) -> (Self, Vec<Receiver<ShardCommand>>) {
        let mut txs = Vec::new();
        let mut rxs = Vec::new();

        for _ in 0..num_shards {
            let (tx, rx) = make_channel(chan_depth);
            txs.push(tx);
            rxs.push(rx);
        }

        (Self { txs }, rxs)
    }

    fn get_shard_id(&self, key: &CacheKey) -> usize {
        let key_hash = XxHash64::oneshot(Self::SEED, key);

        key_hash as usize % self.txs.len()
    }

    fn split_shard_command(&self, command: ShardCommand) -> (Vec<ShardCommand>, Option<Vec<oneshot::Receiver<bool>>>) {
        let mut shard_commands = Vec::new();
        let mut receivers = Vec::new();

        match command {
            ShardCommand::ApplyUpdates { snapshot_vc, updates } => {
                for _ in 0..self.txs.len() {
                    shard_commands.push(ShardCommand::ApplyUpdates { snapshot_vc: snapshot_vc.clone(), updates: Vec::new() });
                }

                for (key, value) in updates {
                    let shard_id = self.get_shard_id(&key);
                    if let ShardCommand::ApplyUpdates { updates, .. } = &mut shard_commands[shard_id] {
                        updates.push((key, value));
                    } else {
                        unreachable!();
                    }
                }
            }
            ShardCommand::VerifyReads { snapshot_vc, reads, .. } => {    
                for _ in 0..self.txs.len() {
                    let (_tx, _rx) = oneshot::channel();
                    shard_commands.push(ShardCommand::VerifyReads { snapshot_vc: snapshot_vc.clone(), reads: Vec::new(), response_tx: Some(_tx) });
                    receivers.push(_rx);
                }

                for (key, value) in reads {
                    let shard_id = self.get_shard_id(&key);
                    if let ShardCommand::VerifyReads { reads, .. } = &mut shard_commands[shard_id] {
                        reads.push((key, value));
                    } else {
                        unreachable!();
                    }
                }
            }
            ShardCommand::CleanSnapshots { snapshot_vc } => {
                for _ in 0..self.txs.len() {
                    shard_commands.push(ShardCommand::CleanSnapshots { snapshot_vc: snapshot_vc.clone() });
                }
            }
        }

        if receivers.is_empty() {
            (shard_commands, None)
        } else {
            (shard_commands, Some(receivers))
        }
    
    }

    async fn send_and_wait(&self, command: ShardCommand) -> Option<bool> {
        let (mut shard_commands, receivers) = self.split_shard_command(command);
        for (i, shard_command) in shard_commands.drain(..).enumerate() {
            self.txs[i].send(shard_command).await.unwrap();
        }

        if let Some(receivers) = receivers {
            let mut result = true;
            for receiver in receivers {
                let res = receiver.await.unwrap();
                result = result && res;
            }

            return Some(result);
        }

        None
    }
}

impl Clone for ShardSender {
    fn clone(&self) -> Self {
        Self { txs: self.txs.clone() }
    }
}

#[derive(Clone)]
struct BlockProcessorCommand {
    block: CachedBlock,
    target_vc: VectorClock,
    response_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl BlockProcessorCommand {
    pub fn new(block: CachedBlock, target_vc: VectorClock) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (Self { block, target_vc, response_tx: Arc::new(Mutex::new(Some(tx))) }, rx)
    }
}

struct BlockProcessor {
    shard_sender: ShardSender,
    command_rx: Receiver<BlockProcessorCommand>,
}

impl BlockProcessor {
    pub fn new(shard_sender: ShardSender, command_rx: Receiver<BlockProcessorCommand>) -> Self {
        Self { shard_sender, command_rx }
    }

    pub async fn run(&mut self) {
        while let Some(command) = self.command_rx.recv().await {
            self.process_block(command).await;
        }
    }

    async fn process_block(&mut self, command: BlockProcessorCommand) {
        let BlockProcessorCommand { block, target_vc, mut response_tx } = command;

        let mut updates = Vec::new();

        for tx in &block.block.tx_list {
            let Some(phase) = &tx.on_crash_commit
            else {
                continue;
            };


            phase.ops.iter()
                .filter_map(|op| process_tx_op(op))
                .for_each(|(key, value)| {
                    updates.push((key, value));
                });
        }

        let shard_command = ShardCommand::ApplyUpdates { snapshot_vc: target_vc, updates };

        let _ = self.shard_sender.send_and_wait(shard_command).await;

        let response_tx = response_tx.lock().await.take().unwrap();
        let _ = response_tx.send(());
    }
}