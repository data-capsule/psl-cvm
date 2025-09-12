use std::{collections::HashSet, fmt::{self, Debug, Display}, ops::{Deref, DerefMut}, pin::Pin, sync::Arc, time::{Duration, Instant}};

use hashbrown::HashMap;
use itertools::Itertools;
use log::{info, trace};
use prost::Message;
use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicPSLWorkerConfig, crypto::{default_hash, CachedBlock, CryptoServiceConnector, FutureHash, HashType}, proto::{consensus::{ProtoBlock, ProtoReadSet, ProtoReadSetEntry, ProtoVectorClock, ProtoVectorClockEntry}, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase}, rpc::ProtoPayload}, rpc::{client::PinnedClient, PinnedMessage, SenderType}, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}};

use super::cache_manager::{CacheKey, CachedValue};

pub enum BlockSeqNumQuery {
    DontBother,
    WaitForSeqNum(oneshot::Sender<u64>),
}

pub enum SequencerCommand {
    /// Write Op from myself
    SelfWriteOp {
        key: CacheKey,
        value: CachedValue,
        seq_num_query: BlockSeqNumQuery,
        current_vc: oneshot::Sender<VectorClock>,
    },

    /// Read Op from myself
    SelfReadOp {
        key: CacheKey,
        value: Option<CachedValue>, // Could be None if the key was not found.
        origin: SenderType,
        snapshot_propagated_signal_tx: Option<oneshot::Sender<()>>,
        current_vc: oneshot::Sender<VectorClock>,
    },

    /// Write Op from other node, that I propagate
    OtherWriteOp {
        key: CacheKey,
        value: CachedValue,
        current_vc: oneshot::Sender<VectorClock>,
    },

    /// Advance the vector clock in the sequencer.
    AdvanceVC {
        sender: SenderType,
        block_seq_num: u64
    },

    /// Blocks can only be formed on receiving this token.
    /// Helps maintain atomicity.
    /// (Doesn't mean it is forced to form a block, just that it can)
    MakeNewBlock,


    /// Force a new block to be formed.
    /// Only if there is at least one write in the bag.
    ForceMakeNewBlock,

    /// Buffer this request, send ack once the VC is >= the one asked for.
    WaitForVC(VectorClock, oneshot::Sender<()>),

    /// Force unblock all buffered waiters >= the given VC.
    UnblockVC(VectorClock),
}

#[derive(Clone)]
pub struct VectorClock(HashMap<SenderType, u64>);

impl Deref for VectorClock {
    type Target = HashMap<SenderType, u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VectorClock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl VectorClock {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn advance(&mut self, sender: SenderType, seq_num: u64) -> bool {
        let entry = self.0.entry(sender).or_insert(0);
        if *entry < seq_num {
            *entry = seq_num;
            true
        } else {
            false
        }
    }

    pub fn get(&self, sender: &SenderType) -> u64 {
        *self.0.get(sender).unwrap_or(&0)
    }

    pub fn serialize(&self) -> ProtoVectorClock {
        ProtoVectorClock {
            entries: self.0.iter().sorted_by_key(|(sender, _)| sender.to_name_and_sub_id().0)
            .map(|(sender, seq_num)| ProtoVectorClockEntry {
                sender: sender.to_name_and_sub_id().0,
                seq_num: *seq_num,
            }).collect(),
        }
    }
}

impl FromIterator<(SenderType, u64)> for VectorClock {
    fn from_iter<T: IntoIterator<Item = (SenderType, u64)>>(iter: T) -> Self {
        let mut vc = VectorClock::new();
        for (sender, seq_num) in iter {
            vc.advance(sender, seq_num);
        }
        vc
    }
}

impl From<Option<ProtoVectorClock>> for VectorClock {
    fn from(vc: Option<ProtoVectorClock>) -> Self {
        match vc {
            Some(proto_vc) => {
                let mut vc = VectorClock::new();
                for entry in proto_vc.entries {
                    vc.advance(SenderType::Auth(entry.sender, 0), entry.seq_num);
                }
                vc
            }
            None => VectorClock::new(),
        }
    }
}

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}

impl Eq for VectorClock {}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Logic:
        // 1. If all entries of self are <= entries of other, return <=
        // 2. If all entries of other are <= entries of self, return >=
        // 3. If there is a mix, return None

        let mut all_self_entries_are_geq = true;
        let mut all_self_entries_are_leq = true;

        for (sender, self_seq_num) in self.0.iter() {
            let other_seq_num = other.get(sender);
            if *self_seq_num > other_seq_num {
                all_self_entries_are_leq = false;
            }

            if *self_seq_num < other_seq_num {
                all_self_entries_are_geq = false;
            }
        }

        for (sender, other_seq_num) in other.0.iter() {
            let self_seq_num = self.get(sender);
            if self_seq_num > *other_seq_num {
                all_self_entries_are_leq = false;
            }

            if self_seq_num < *other_seq_num {
                all_self_entries_are_geq = false;
            }
        }

        // This is done because self and other may not have the same set of senders.

        if all_self_entries_are_geq && all_self_entries_are_leq {
            return Some(std::cmp::Ordering::Equal);
        }

        if all_self_entries_are_geq {
            return Some(std::cmp::Ordering::Greater);
        }

        if all_self_entries_are_leq {
            return Some(std::cmp::Ordering::Less);
        }

        return None;
    }
}

impl std::hash::Hash for VectorClock {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.serialize().encode_to_vec().hash(state);
    }
}

impl Display for VectorClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<")?;
        for (sender, seq_num) in self.0.iter().sorted_by_key(|(sender, _)| sender.to_name_and_sub_id().0) {
            let (name, _) = sender.to_name_and_sub_id();
            write!(f, "{} -> {} ", name, seq_num)?;
        }
        write!(f, ">")?;

        Ok(())
    }
}

impl Debug for VectorClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<")?;
        for (sender, seq_num) in self.0.iter().sorted_by_key(|(sender, _)| sender.to_name_and_sub_id().0) {
            let (name, _) = sender.to_name_and_sub_id();
            write!(f, "{} -> {} ", name, seq_num)?;
        }
        write!(f, ">")?;

        Ok(())
    }
}


pub struct BlockSequencer {
    config: AtomicPSLWorkerConfig,
    crypto: CryptoServiceConnector,
    client: PinnedClient,

    curr_block_seq_num: u64,
    last_block_hash: FutureHash,
    self_write_op_bag: Vec<(CacheKey, CachedValue)>,
    dirty_keys: HashSet<CacheKey>,
    all_write_op_bag: Vec<(CacheKey, CachedValue)>,
    self_read_op_bag: Vec<(CacheKey, Option<CachedValue>, SenderType)>,

    curr_vector_clock: VectorClock,
    __vc_dirty: bool,

    cache_manager_rx: Receiver<SequencerCommand>,

    node_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
    storage_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    vc_wait_buffer: HashMap<VectorClock, Vec<oneshot::Sender<()>>>,

    chain_id: u64, // This is unused for PSL, but useful for Nimble port.

    snapshot_propagated_signal_tx: Vec<oneshot::Sender<()>>,

    last_heartbeat_time: Instant,
    __num_times_blocked: usize,

    must_flush_before_next_other_write_op: bool,
}

impl BlockSequencer {
    pub fn new(config: AtomicPSLWorkerConfig, crypto: CryptoServiceConnector,
        client: PinnedClient,
        cache_manager_rx: Receiver<SequencerCommand>,
        node_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
        storage_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
        chain_id: u64,
    ) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config, crypto, client,
            curr_block_seq_num: 0,
            last_block_hash: FutureHash::Immediate(default_hash()),
            self_write_op_bag: Vec::new(),
            dirty_keys: HashSet::new(),
            all_write_op_bag: Vec::new(),
            self_read_op_bag: Vec::new(),
            curr_vector_clock: VectorClock::new(),
            cache_manager_rx,
            node_broadcaster_tx,
            storage_broadcaster_tx,
            log_timer,
            vc_wait_buffer: HashMap::new(),
            chain_id,
            snapshot_propagated_signal_tx: Vec::new(),
            __vc_dirty: false,
            last_heartbeat_time: Instant::now(),
            __num_times_blocked: 0,
            must_flush_before_next_other_write_op: false,
        }
    }

    pub async fn run(block_sequencer: Arc<Mutex<Self>>) {
        let mut block_sequencer = block_sequencer.lock().await;
        block_sequencer.log_timer.run().await;
        block_sequencer.worker().await;
    }

    async fn worker(&mut self) {
        loop {
            tokio::select! {
                command = self.cache_manager_rx.recv() => {
                    self.handle_command(command.unwrap()).await;
                }
                _ = self.log_timer.wait() => {
                    self.log_stats().await;
                }
            }
        }
    }

    async fn log_stats(&mut self) {
        info!("Vector Clock: {}, Blocked times: {}", self.curr_vector_clock, self.__num_times_blocked);
    }

    async fn handle_command(&mut self, command: SequencerCommand) {
        match command {
            SequencerCommand::SelfWriteOp { key, value, seq_num_query, current_vc } => {
                self.self_write_op_bag.push((key.clone(), value.clone()));
                self.all_write_op_bag.push((key.clone(), value));
                self.dirty_keys.insert(key);

                match seq_num_query {
                    BlockSeqNumQuery::DontBother => {}
                    BlockSeqNumQuery::WaitForSeqNum(sender) => {
                        sender.send(self.curr_block_seq_num).unwrap();
                    }
                }
                current_vc.send(self.curr_vector_clock.clone()).unwrap();
            },
            SequencerCommand::SelfReadOp { key, value, origin, snapshot_propagated_signal_tx, current_vc } => {
                if !self.dirty_keys.contains(&key) {
                    self.self_read_op_bag.push((key, value, origin));
                }
                current_vc.send(self.curr_vector_clock.clone()).unwrap();
                if let Some(tx) = snapshot_propagated_signal_tx {
                    self.snapshot_propagated_signal_tx.push(tx);
                }
                self.must_flush_before_next_other_write_op = true;
            },
            SequencerCommand::OtherWriteOp { key, value, current_vc } => {
                
                // All OtherWriteOps come before any SelfReadOp for a given block.
                // If I have SelfWriteOp(x) <-- OtherWriteOp(x) <-- SelfReadOp(x),
                // I want to unmark x as dirty.
                // self.dirty_keys.remove(&key);

                while self.must_flush_before_next_other_write_op {
                    // Pretend there is a ForceMakeNewBlock before this.
                    self.force_prepare_new_block().await;
                }
                
                self.all_write_op_bag.push((key, value));
                current_vc.send(self.curr_vector_clock.clone()).unwrap();
            },
            SequencerCommand::AdvanceVC { sender, block_seq_num } => {
                while self.must_flush_before_next_other_write_op {
                    // Pretend there is a ForceMakeNewBlock before this.
                    self.force_prepare_new_block().await;
                }
                assert!(self.self_read_op_bag.is_empty());
                
                if block_seq_num == 0 {
                    // We want to keep the succint reprensentation.
                    return;
                }

                if self.curr_vector_clock.advance(sender, block_seq_num) {
                    self.__vc_dirty = true;
                }
                self.send_heartbeat().await;
                self.flush_vc_wait_buffer().await;
            },
            SequencerCommand::MakeNewBlock => {
                self.send_heartbeat().await;
                self.maybe_prepare_new_block().await;
            },
            SequencerCommand::ForceMakeNewBlock => {
                self.send_heartbeat().await;
                self.force_prepare_new_block().await;
            },
            SequencerCommand::WaitForVC(vc, sender) => {
                self.buffer_vc_wait(vc, sender).await;
                self.__num_times_blocked += 1;
            }
            SequencerCommand::UnblockVC(vc) => {
                self._flush_vc_wait_buffer(vc);
            }
        }
    }

    async fn maybe_prepare_new_block(&mut self) {
        let config = self.config.get();
        let all_write_batch_size = config.worker_config.all_writes_max_batch_size;
        let self_write_batch_size = config.worker_config.self_writes_max_batch_size;
        let self_read_batch_size = config.worker_config.self_reads_max_batch_size;

        if self.all_write_op_bag.len() < all_write_batch_size
        || self.self_write_op_bag.len() < self_write_batch_size 
        || self.self_read_op_bag.len() < self_read_batch_size
        {
            return; // Not enough writes to form a block
        }

        if self.self_write_op_bag.is_empty() && self.self_read_op_bag.is_empty() {
            self.forward_just_other_writes().await;
            return;
        }

        self.do_prepare_new_block().await;
    }

    async fn force_prepare_new_block(&mut self) {
        // Force to send null blocks if needed.
        trace!("Force preparing new block. VC dirty: {} , all_write_op_bag: {}, self_write_op_bag: {}, self_read_op_bag: {}",
            self.__vc_dirty, self.all_write_op_bag.len(), self.self_write_op_bag.len(), self.self_read_op_bag.len());

        // if self.all_write_op_bag.is_empty() && self.self_read_op_bag.is_empty() && !self.__vc_dirty {
        //     return;
        // }

        if self.self_write_op_bag.is_empty() && self.self_read_op_bag.is_empty() {
            self.forward_just_other_writes().await;
            return;
        }
        
        self.do_prepare_new_block().await;
    }

    async fn send_heartbeat(&mut self) {
        let heartbeat_timeout = self.last_heartbeat_time.elapsed().as_millis() >= self.config.get().worker_config.heartbeat_max_delay_ms as u128;
        // let i_am_blocked = self.vc_wait_buffer.len() > 0;
        let i_am_blocked = false;
        
        if !(heartbeat_timeout || i_am_blocked) {
            return;
        }

        self.last_heartbeat_time = Instant::now();

        trace!("Sending heartbeat with vc: {:?} heartbeat_timeout: {} i_am_blocked: {}", self.curr_vector_clock, heartbeat_timeout, i_am_blocked);
        let vc = self.curr_vector_clock.clone();
        let vc = vc.serialize();
        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::Heartbeat(vc)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        let _ = PinnedClient::send(&self.client, &"sequencer1".to_string(), request.as_ref()).await;
    }



    /// Same as do_prepare_new_block, but without self writes and reads.
    /// Doesn't increment the seq_num.
    /// This helps to synchronize the read_vc of all workers.
    async fn forward_just_other_writes(&mut self) {
        let seq_num = self.curr_block_seq_num;

        let all_writes = Self::wrap_vec(
            Self::dedup_vec(self.all_write_op_bag.drain(..)),
            vec![], // Reads are not forwarded to other nodes.
            seq_num,
            Some(self.curr_vector_clock.serialize()),
            String::from("god"),
            self.chain_id,
        );

        let (all_writes_rx, _, _) = self.crypto.prepare_block(
            all_writes,
            false,
            FutureHash::Immediate(default_hash())
        ).await;

        let _ = self.node_broadcaster_tx.send(all_writes_rx).await;

        self.__vc_dirty = false;
    }

    async fn do_prepare_new_block(&mut self) {
        self.curr_block_seq_num += 1;
        let seq_num = self.curr_block_seq_num;


        let origin = self.config.get().net_config.name.clone();
        let me = SenderType::Auth(origin.clone(), 0);
        let read_vc = self.curr_vector_clock.clone();
        self.curr_vector_clock.advance(me.clone(), seq_num);
        assert!(read_vc.get(&me) + 1 == seq_num);

        let self_reads = self.self_read_op_bag.drain(..).collect::<Vec<_>>();

        let all_writes = Self::wrap_vec(
            Self::dedup_vec(self.all_write_op_bag.drain(..)),
            vec![], // Reads are not forwarded to other nodes.
            seq_num,
            Some(self.curr_vector_clock.serialize()),
            origin.clone(),
            self.chain_id,
        );

        let self_writes_and_reads = Self::wrap_vec(
            Self::dedup_vec(self.self_write_op_bag.drain(..)),
            Self::prepare_read_set(self_reads, &self.dirty_keys),
            seq_num,
            Some(read_vc.serialize()),
            origin,
            self.chain_id,
        );
        self.dirty_keys.clear();


        let (all_writes_rx, _, _) = self.crypto.prepare_block(
            all_writes,
            false,
            FutureHash::Immediate(default_hash())
        ).await;

        let parent_hash_rx = self.last_block_hash.take();
        let (self_writes_rx, hash_rx, hash_rx2) = self.crypto.prepare_block(
            self_writes_and_reads,
            true,
            parent_hash_rx,
        ).await;
        self.last_block_hash = FutureHash::Future(hash_rx);

        // Nodes get all writes so as to virally send writes from other nodes.
        let _ = self.node_broadcaster_tx.send(all_writes_rx).await;

        // Storage only gets self writes.
        // Strong convergence will ensure that the checkpoint state matches the state using all_writes above.
        // Same VC => Same state.
        let _ = self.storage_broadcaster_tx.send(self_writes_rx).await;

        // TODO: Send hash_rx2 to client reply handler.

        // Signal the cache manager so that it can start processing blocks from others.
        for tx in self.snapshot_propagated_signal_tx.drain(..) {
            let _ = tx.send(());
        }

        self.__vc_dirty = false;

        self.must_flush_before_next_other_write_op = false;
    }

    fn wrap_vec(
        writes: Vec<(CacheKey, CachedValue)>,
        reads: Vec<(CacheKey, HashType, String)>,
        seq_num: u64,
        vector_clock: Option<ProtoVectorClock>,
        origin: String,
        chain_id: u64,
    ) -> ProtoBlock {
        ProtoBlock {
            tx_list: writes.into_iter()
                .map(|(key, value)| ProtoTransaction {
                    on_receive: None,
                    on_crash_commit: Some(ProtoTransactionPhase {
                        ops: vec![ProtoTransactionOp { 
                            op_type: ProtoTransactionOpType::Write as i32,
                            operands: vec![key, bincode::serialize(&value).unwrap()], 
                        }],
                    }),
                    on_byzantine_commit: None,
                    is_reconfiguration: false,
                    is_2pc: false,
                })
                .collect(),
            n: seq_num,
            parent: vec![],
            view: 0,
            qc: vec![],
            fork_validation: vec![],
            view_is_stable: true,
            config_num: 1,
            sig: None,
            vector_clock,
            origin,
            chain_id, // This field is generally unused.

            read_set: Some(ProtoReadSet {
                entries: reads.into_iter()
                    .map(|(key, value_hash, origin)| ProtoReadSetEntry {
                        key,
                        value_hash,
                        origin,
                    })
                    .collect(),
                merkle_root: vec![],
            })
        }
    }

    fn dedup_vec(vec: std::vec::Drain<(CacheKey, CachedValue)>) -> Vec<(CacheKey, CachedValue)> {
        let mut seen = HashMap::new();

        for (key, value) in vec {
            let entry = seen.entry(key).or_insert(value.clone());

            entry.merge_cached(value);
        }

        seen.into_iter().collect()
    }


    async fn flush_vc_wait_buffer(&mut self) {
        // if self.vc_wait_buffer.len() > 0 {
        //     warn!("Current VC: {}, VC wait buffer: {:?}", self.curr_vector_clock, self.vc_wait_buffer);
        // }

        self._flush_vc_wait_buffer(self.curr_vector_clock.clone());
    }
    
    fn _flush_vc_wait_buffer(&mut self, target_vc: VectorClock) {
        // let i_was_blocked = self.vc_wait_buffer.len() > 0;
        let mut to_remove = Vec::new();
        for (vc, _) in self.vc_wait_buffer.iter() {
            if target_vc >= *vc {
                to_remove.push(vc.clone());
            }
        }

        for vc in to_remove.iter() {
            let mut senders = self.vc_wait_buffer.remove(vc).unwrap();
            for sender in senders.drain(..) {                
                let _ = sender.send(());
            }
        }
        // let i_am_blocked = self.vc_wait_buffer.len() > 0;

        // if i_was_blocked && !i_am_blocked {
        //     warn!("Unblocked");
        // }
    }

    async fn buffer_vc_wait(&mut self, vc: VectorClock, sender: oneshot::Sender<()>) {
        // let i_was_blocked = self.vc_wait_buffer.len() > 0;
        let buffer = self.vc_wait_buffer.entry(vc).or_insert(Vec::new());
        buffer.push(sender);

        self.flush_vc_wait_buffer().await;
        // let i_am_blocked = self.vc_wait_buffer.len() > 0;

        // if !i_was_blocked && i_am_blocked {
        //     warn!("Blocked");
        // }
    }

    fn prepare_read_set(reads: Vec<(CacheKey, Option<CachedValue>, SenderType)>, dirty_keys: &HashSet<CacheKey>) -> Vec<(CacheKey, HashType, String)> {
        reads.into_iter()
            .filter_map(|(key, value, origin)| {
                if dirty_keys.contains(&key) {
                    None
                } else {
                    let val_hash = cached_value_to_val_hash(value);
                    let origin = origin.to_name_and_sub_id().0;
                    Some((key, val_hash, origin))
                }
            })
            .sorted_by_key(|(key, _, _)| key.clone())
            .collect()
    }
}

pub fn cached_value_to_val_hash(value: Option<CachedValue>) -> HashType {
    match value {
        Some(value) => value.val_hash.to_bytes_be().1,
        None => vec![],
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_ordering() {
        let vc1 = VectorClock::from_iter(vec![
            (SenderType::Auth("A".to_string(), 0), 1),
            (SenderType::Auth("B".to_string(), 0), 2),
            (SenderType::Auth("C".to_string(), 0), 3),
        ]);

        let vc2 = VectorClock::from_iter(vec![
            (SenderType::Auth("A".to_string(), 0), 2),
            (SenderType::Auth("B".to_string(), 0), 2),
            (SenderType::Auth("C".to_string(), 0), 4),
        ]);

        let vc3 = VectorClock::from_iter(vec![
            (SenderType::Auth("A".to_string(), 0), 2),
            (SenderType::Auth("B".to_string(), 0), 2),
            (SenderType::Auth("C".to_string(), 0), 2),
        ]);

        assert!(vc1 <= vc2);
        assert!(vc2 >= vc1);
        assert!(vc1 < vc2);

        assert!(!(vc1 < vc3 && vc3 < vc1));
        assert!(vc1 != vc3);
    }

    #[test]
    fn vector_clock_missing_entries() {
        let vc1 = VectorClock::from_iter(vec![
            (SenderType::Auth("A".to_string(), 0), 2),
            (SenderType::Auth("B".to_string(), 0), 2),
        ]);

        let vc2 = VectorClock::from_iter(vec![
            (SenderType::Auth("A".to_string(), 0), 2),
            (SenderType::Auth("B".to_string(), 0), 2),
            (SenderType::Auth("C".to_string(), 0), 4),
        ]);

        let vc3 = VectorClock::new();

        assert!(vc1 < vc2);
        assert!(vc2 > vc1);

        assert!(vc3 < vc1);
        assert!(vc3 < vc2);
        assert!(!(vc3 < vc3));
        assert!(vc1 > vc3);
    }
}