use std::{fmt::{self, Display, Debug}, ops::{Deref, DerefMut}, pin::Pin, sync::Arc, time::Duration};

use hashbrown::HashMap;
use itertools::Itertools;
use log::info;
use prost::Message;
use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicPSLWorkerConfig, crypto::{default_hash, CachedBlock, CryptoServiceConnector, FutureHash}, proto::{consensus::{ProtoBlock, ProtoVectorClock, ProtoVectorClockEntry}, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase}}, rpc::SenderType, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}};

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
    },

    /// Write Op from other node, that I propagate
    OtherWriteOp {
        key: CacheKey,
        value: CachedValue,
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

    pub fn advance(&mut self, sender: SenderType, seq_num: u64) {
        let entry = self.0.entry(sender).or_insert(0);
        if *entry < seq_num {
            *entry = seq_num;
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

    curr_block_seq_num: u64,
    last_block_hash: FutureHash,
    self_write_op_bag: Vec<(CacheKey, CachedValue)>,
    all_write_op_bag: Vec<(CacheKey, CachedValue)>,
    curr_vector_clock: VectorClock,

    cache_manager_rx: Receiver<SequencerCommand>,

    node_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
    storage_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    vc_wait_buffer: HashMap<VectorClock, Vec<oneshot::Sender<()>>>,

    chain_id: u64, // This is unused for PSL, but useful for Nimble port.
}

impl BlockSequencer {
    pub fn new(config: AtomicPSLWorkerConfig, crypto: CryptoServiceConnector,
        cache_manager_rx: Receiver<SequencerCommand>,
        node_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
        storage_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
        chain_id: u64,
    ) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config, crypto,
            curr_block_seq_num: 1,
            last_block_hash: FutureHash::Immediate(default_hash()),
            self_write_op_bag: Vec::new(),
            all_write_op_bag: Vec::new(),
            curr_vector_clock: VectorClock::new(),
            cache_manager_rx,
            node_broadcaster_tx,
            storage_broadcaster_tx,
            log_timer,
            vc_wait_buffer: HashMap::new(),
            chain_id,
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
        info!("Vector Clock: {}", self.curr_vector_clock);
    }

    async fn handle_command(&mut self, command: SequencerCommand) {
        match command {
            SequencerCommand::SelfWriteOp { key, value, seq_num_query } => {
                self.self_write_op_bag.push((key.clone(), value.clone()));
                self.all_write_op_bag.push((key, value));

                match seq_num_query {
                    BlockSeqNumQuery::DontBother => {}
                    BlockSeqNumQuery::WaitForSeqNum(sender) => {
                        sender.send(self.curr_block_seq_num).unwrap();
                    }
                }
            },
            SequencerCommand::OtherWriteOp { key, value } => {
                self.all_write_op_bag.push((key, value));
            },
            SequencerCommand::AdvanceVC { sender, block_seq_num } => {
                self.curr_vector_clock.advance(sender, block_seq_num);
                self.flush_vc_wait_buffer().await;
            },
            SequencerCommand::MakeNewBlock => {
                self.maybe_prepare_new_block().await;
            },
            SequencerCommand::ForceMakeNewBlock => {
                self.force_prepare_new_block().await;
            },
            SequencerCommand::WaitForVC(vc, sender) => {
                self.buffer_vc_wait(vc, sender).await;
            }
        }
    }

    async fn maybe_prepare_new_block(&mut self) {
        let config = self.config.get();
        let all_write_batch_size = config.worker_config.all_writes_max_batch_size;
        let self_write_batch_size = config.worker_config.self_writes_max_batch_size;

        if self.all_write_op_bag.len() < all_write_batch_size  || self.self_write_op_bag.len() < self_write_batch_size {
            return; // Not enough writes to form a block
        }

        self.do_prepare_new_block().await;
    }

    async fn force_prepare_new_block(&mut self) {
        if self.all_write_op_bag.is_empty() {
            return;
        }
        
        self.do_prepare_new_block().await;
    }

    async fn do_prepare_new_block(&mut self) {
        let seq_num = self.curr_block_seq_num;
        self.curr_block_seq_num += 1;


        let origin = self.config.get().net_config.name.clone();
        let me = SenderType::Auth(origin.clone(), 0);
        let read_vc = self.curr_vector_clock.clone();
        self.curr_vector_clock.advance(me.clone(), seq_num);
        assert!(read_vc.get(&me) + 1 == seq_num);

        let all_writes = Self::wrap_vec(
            Self::dedup_vec(self.all_write_op_bag.drain(..)),
            seq_num,
            Some(self.curr_vector_clock.serialize()),
            origin.clone(),
            self.chain_id,
        );

        let self_writes = Self::wrap_vec(
            Self::dedup_vec(self.self_write_op_bag.drain(..)),
            seq_num,
            Some(read_vc.serialize()),
            origin,
            self.chain_id,
        );


        let (all_writes_rx, _, _) = self.crypto.prepare_block(
            all_writes,
            false,
            FutureHash::Immediate(default_hash())
        ).await;

        let parent_hash_rx = self.last_block_hash.take();
        let (self_writes_rx, hash_rx, hash_rx2) = self.crypto.prepare_block(
            self_writes,
            true,
            parent_hash_rx,
        ).await;
        self.last_block_hash = FutureHash::Future(hash_rx);

        // Nodes get all writes so as to virally send writes from other nodes.
        self.node_broadcaster_tx.send(all_writes_rx).await;

        // Storage only gets self writes.
        // Strong convergence will ensure that the checkpoint state matches the state using all_writes above.
        // Same VC => Same state.
        self.storage_broadcaster_tx.send(self_writes_rx).await;

        // TODO: Send hash_rx2 to client reply handler.
    }

    fn wrap_vec(
        writes: Vec<(CacheKey, CachedValue)>,
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
        let mut to_remove = Vec::new();

        for (vc, _) in self.vc_wait_buffer.iter() {
            if self.curr_vector_clock >= *vc {
                to_remove.push(vc.clone());
            }
        }

        for vc in to_remove.iter() {
            let mut senders = self.vc_wait_buffer.remove(vc).unwrap();
            for sender in senders.drain(..) {
                let _ = sender.send(());
            }
        }
    }

    async fn buffer_vc_wait(&mut self, vc: VectorClock, sender: oneshot::Sender<()>) {
        let buffer = self.vc_wait_buffer.entry(vc).or_insert(Vec::new());
        buffer.push(sender);

        self.flush_vc_wait_buffer().await;
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