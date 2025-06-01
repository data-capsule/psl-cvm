use std::ops::{Deref, DerefMut};

use hashbrown::HashMap;

use crate::{config::AtomicConfig, crypto::{CryptoServiceConnector, FutureHash, HashType}, rpc::SenderType, utils::channel::Receiver};

use super::cache_manager::CacheKey;

pub enum SequencerCommand {
    /// Write Op from myself
    SelfWriteOp {
        key: Vec<u8>,
        value: Vec<u8>,
        seq_num: u64,
    },

    /// Write Op from other node, that I propagate
    OtherWriteOp {
        key: Vec<u8>,
        value: Vec<u8>,
        seq_num: u64,
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
}


struct VectorClock(HashMap<SenderType, u64>);

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

    pub fn get(&self, sender: &SenderType) -> Option<&u64> {
        self.0.get(sender)
    }
}


pub struct BlockSequencer {
    config: AtomicConfig,
    crypto: CryptoServiceConnector,

    curr_block_seq_num: u64,
    last_block_hash: FutureHash,
    self_write_op_bag: Vec<(CacheKey, Vec<u8>, u64)>,
    all_write_op_bag: Vec<(CacheKey, Vec<u8>, u64)>,
    curr_vector_clock: VectorClock,

    cache_manager_rx: Receiver<SequencerCommand>,
}

impl BlockSequencer {
    pub fn new(config: AtomicConfig, crypto: CryptoServiceConnector, cache_manager_rx: Receiver<SequencerCommand>) -> Self {
        Self {
            config, crypto,
            curr_block_seq_num: 0,
            last_block_hash: FutureHash::Immediate(HashType::default()),
            self_write_op_bag: Vec::new(),
            all_write_op_bag: Vec::new(),
            curr_vector_clock: VectorClock::new(),
            cache_manager_rx,
        }
    }

    pub async fn run(&mut self) {
        while let Some(command) = self.cache_manager_rx.recv().await {
            match command {
                SequencerCommand::SelfWriteOp { key, value, seq_num } => {
                    self.self_write_op_bag.push((key.clone(), value.clone(), seq_num));
                    self.all_write_op_bag.push((key, value, seq_num));
                },
                SequencerCommand::OtherWriteOp { key, value, seq_num } => {
                    self.all_write_op_bag.push((key, value, seq_num));
                },
                SequencerCommand::AdvanceVC { sender, block_seq_num } => {
                    self.curr_vector_clock.advance(sender, block_seq_num);
                },
                SequencerCommand::MakeNewBlock => {
                    self.maybe_prepare_new_block().await;
                },
            }
        }
    }

    async fn maybe_prepare_new_block(&mut self) {
        let config = self.config.get();
        // TODO: Make the config proper.
        let all_write_batch_size = config.consensus_config.max_backlog_batch_size;
        let self_write_batch_size = all_write_batch_size / 2;

        if self.all_write_op_bag.len() < all_write_batch_size  || self.self_write_op_bag.len() < self_write_batch_size {
            return; // Not enough writes to form a block
        }

        let all_writes = Self::dedup_vec(self.all_write_op_bag.drain(..));
        let self_writes = Self::dedup_vec(self.self_write_op_bag.drain(..));

        // TODO: Directly send the all_writes block to the gossip broadacaster.

        // TODO: Send self_writes to crypto and the resulting future to storage broadcaster.
    }

    fn dedup_vec(vec: std::vec::Drain<(CacheKey, Vec<u8>, u64)>) -> Vec<(CacheKey, Vec<u8>, u64)> {
        let mut seen = HashMap::new();

        for (key, value, seq_num) in vec {
            let entry = seen.entry(key).or_insert((value.clone(), seq_num));

            if entry.1 < seq_num {
                entry.0 = value;
                entry.1 = seq_num;
            }
        }

        seen.into_iter().map(|(key, (value, seq_num))| (key, value, seq_num)).collect()
    }
}