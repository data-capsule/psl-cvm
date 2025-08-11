use std::collections::{HashMap, VecDeque};

use crate::{config::AtomicConfig, crypto::CachedBlock, rpc::SenderType, sequencer::{commit_buffer::BlockStats, controller::ControllerCommand}, utils::channel::{Receiver, Sender}, worker::{block_sequencer::VectorClock, cache_manager::{CacheKey, CachedValue}}};

pub struct Auditor {
    config: AtomicConfig,
    auditor_rx: Receiver<(BlockStats, CachedBlock)>,
    controller_tx: Sender<ControllerCommand>,

    unaudited_buffer: HashMap<
        SenderType,
        VecDeque<(BlockStats, CachedBlock)>
    >,
    unaudited_buffer_size: usize,

    state_snapshots: HashMap<
        CacheKey,
        VecDeque<(VectorClock, CachedValue)>
    >,

    snapshot_vcs: VecDeque<VectorClock>,
}