use std::{collections::HashSet, fs::File, io::{BufWriter, Write}, pin::Pin, sync::Arc, time::{Duration, Instant}};

use hashbrown::HashMap;
use log::{error, info, trace, warn};
use num_bigint::{BigInt, Sign};
#[cfg(feature = "evil")]
use rand::distr::{Distribution, weighted::WeightedIndex};
use thiserror::Error;
use tokio::sync::{mpsc::{UnboundedReceiver, UnboundedSender}, oneshot::{self, error::RecvError}, Mutex};
use crate::{config::AtomicPSLWorkerConfig, crypto::{hash, CachedBlock}, proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType}, rpc::SenderType, storage_server::fork_receiver::ForkReceiverCommand, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::{cached_value_to_val_hash, AdvanceVCCommand, BlockSeqNumQuery, VectorClock}, TxWithAckChanTag}};
use crate::worker::block_sequencer::SequencerCommand;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Internal error")]
    InternalError,

    #[error("Lock not acquirable")]
    LockNotAcquirable,

    #[error("Type mismatch")]
    TypeMismatch,
}

#[derive(Debug)]
pub enum CacheCommand {
    Get(CacheKey /* Key */, bool /* block snapshot */, BlockSeqNumQuery, oneshot::Sender<Result<CachedValue, CacheError>>,),
    Put(
        CacheKey /* Key */,
        CachedValue /* Value */,
        BlockSeqNumQuery,
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    ),
    Increment(
        CacheKey /* Key */,
        f64 /* Value */,
        BlockSeqNumQuery,
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    ),
    Decrement(
        CacheKey /* Key */,
        f64 /* Value */,
        BlockSeqNumQuery,
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    ),
    Cas(
        CacheKey /* Key */,
        CachedValue /* Value */,
        u64 /* Expected SeqNum */,
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    ),
    Commit(oneshot::Sender<VectorClock>, bool /* force prepare */),
    WaitForVC(VectorClock, oneshot::Sender<()>),
    TransparentWaitForVC(VectorClock, oneshot::Sender<()>),
    ClearVC(VectorClock),
    QueryVC(oneshot::Sender<VectorClock>),
    
}


#[derive(Clone)]
pub struct CacheConnector {
    request_tx: Sender<CacheCommand>,
}

macro_rules! dispatch {
    ($self: expr, $cmd: expr, $($args: expr),+) => {
        {
            let (response_tx, response_rx) = oneshot::channel();
            $self.dispatch_nonblocking($cmd($($args),+, response_tx)).await?;
    
            match response_rx.await {
                Ok(ret) => ret?,
                Err(e) => panic!("Cache error: {}", e),
            }
        }
    };
}

impl CacheConnector {
    pub fn new(request_tx: Sender<CacheCommand>) -> Self {
        Self { request_tx }
    }

    pub async fn dispatch_nonblocking(
        &self,
        command: CacheCommand,
    ) -> anyhow::Result<()> {
        self.request_tx.send(command).await?;
        Ok(())
    }

    pub async fn get(
        &self,
        key: Vec<u8>,
        seq_num_query: BlockSeqNumQuery,
    ) -> anyhow::Result<CachedValue> {
        let res = dispatch!(self, CacheCommand::Get, key, true, seq_num_query);
        Ok(res)
    }

    pub async fn get_nonblocking(
        &self,
        key: Vec<u8>,
        seq_num_query: BlockSeqNumQuery,
    ) -> anyhow::Result<CachedValue> {
        let res = dispatch!(self, CacheCommand::Get, key, false, seq_num_query);
        Ok(res)
    }

    pub async fn put(
        &self,
        key: Vec<u8>,
        value: CachedValue,
        seq_num_query: BlockSeqNumQuery,
    ) -> anyhow::Result<()> {
        dispatch!(self, CacheCommand::Put, key, value, seq_num_query);
        Ok(())
    }


}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DWWValue {
    pub(crate) value: Vec<u8>,
    pub(crate) seq_num: u64,
    pub(crate) val_hash: BigInt,
}

impl DWWValue {

    pub fn get_value(&self) -> Vec<u8> {
        self.value.clone()
    }

    /// Size estimate in bytes.
    pub fn size(&self) -> usize {
        self.value.len() + std::mem::size_of::<u64>() + self.val_hash.to_bytes_be().1.len()
    }

    /// Completely new value, with seq_num = 1
    pub fn new(value: Vec<u8>, val_hash: BigInt) -> Self {
        Self::new_with_seq_num(value, 1, val_hash)
    }

    pub fn new_with_seq_num(value: Vec<u8>, seq_num: u64, val_hash: BigInt) -> Self {
        Self {
            value,
            seq_num,
            val_hash,
        }
    }

    /// Blindly update value, incrementing seq_num
    pub fn blind_update(&mut self, new_value: Vec<u8>, new_val_hash: BigInt) -> u64 {
        self.value = new_value;
        self.seq_num += 1;
        self.val_hash = new_val_hash;

        self.seq_num
    }


    /// Merge with new value with new seq_num.
    /// Merge logic is: Higher seq_num or same seq_num with higher hash.
    /// Hash is calculated only when necessary. So this is not constant time.
    /// Returns Ok(new_seq_num) | Err(old_seq_num)
    pub fn merge(&mut self, new_value: Vec<u8>, new_seq_num: u64) -> Result<u64, u64> {
        if new_seq_num > self.seq_num {
            self.value = new_value;
            self.seq_num = new_seq_num;
            self.val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&self.value));
            return Ok(new_seq_num);
        } else if new_seq_num == self.seq_num {
            let new_hash = hash(&new_value);
            let new_hash_num = BigInt::from_bytes_be(Sign::Plus, &new_hash);
            if new_hash_num > self.val_hash {
                self.value = new_value;
                self.val_hash = new_hash_num;
                self.seq_num = new_seq_num;
                return Ok(new_seq_num);
            }
        }

        Err(self.seq_num)
    }


    /// This is the same as merge, but with CachedValue as input.
    pub fn merge_cached(
        &mut self,
        new_value: Self,
    ) -> Result<u64, u64> {
        if new_value.seq_num > self.seq_num {
            self.value = new_value.value;
            self.seq_num = new_value.seq_num;
            self.val_hash = new_value.val_hash;
            return Ok(self.seq_num);
        } else if new_value.seq_num == self.seq_num {
            if new_value.val_hash > self.val_hash {
                self.value = new_value.value;
                self.val_hash = new_value.val_hash;
                self.seq_num = new_value.seq_num;
                return Ok(self.seq_num);
            }
        }

        Err(self.seq_num)
    }

    pub fn merge_immutable(&self, new_value: &Self) -> Self {
        let mut val = self.clone();
        let _ = val.merge_cached(new_value.clone());
        val
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]

pub struct PNCounterValue {
    pub(crate) increment_value: HashMap<String /* origin */, f64>,
    pub(crate) decrement_value: HashMap<String /* origin */, f64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]

pub struct WrongPNCounterValue {
    pub(crate) value: f64,
}

impl PNCounterValue {
    pub fn new() -> Self {
        Self {
            increment_value: HashMap::new(),
            decrement_value: HashMap::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.increment_value.iter().map(|(key, _)| key.len() + std::mem::size_of::<f64>()).sum::<usize>()
        + self.decrement_value.iter().map(|(key, _)| key.len() + std::mem::size_of::<f64>()).sum::<usize>()
    }

    pub fn get_value(&self) -> f64 {
        let incr_value_total = self.increment_value.values().sum::<f64>();
        let decr_value_total = self.decrement_value.values().sum::<f64>();
        incr_value_total - decr_value_total
    }

    pub fn merge(&mut self, new_value: Self) -> Result<f64, f64> {
        let mut any_change = false;
        let all_incr_keys = self.increment_value.keys().chain(new_value.increment_value.keys()).cloned().collect::<HashSet<_>>();
        for key in &all_incr_keys {
            let val1 = *self.increment_value.get(key).unwrap_or(&0.0);
            let val2 = *new_value.increment_value.get(key).unwrap_or(&0.0);

            let final_val = val1.max(val2);
            if (final_val - val1) > 1e-6 {
                any_change = true;
            }
            self.increment_value.insert(key.clone(), final_val);
        }

        let all_decr_keys = self.decrement_value.keys().chain(new_value.decrement_value.keys()).cloned().collect::<HashSet<_>>();
        for key in &all_decr_keys {
            let val1 = *self.decrement_value.get(key).unwrap_or(&0.0);
            let val2 = *new_value.decrement_value.get(key).unwrap_or(&0.0);
            let final_val = val1.max(val2);
            if (final_val - val1) > 1e-6 {
                any_change = true;
            }
            self.decrement_value.insert(key.clone(), final_val);
        }

        // warn!("Merged PNCounterValue: {:?}", self);

        if any_change {
            Ok(self.get_value())
        } else {
            Err(self.get_value())
        }


    }

    pub fn merge_immutable(&self, new_value: &Self) -> Self {
        let mut val = self.clone();
        val.merge(new_value.clone());
        val
    }

    pub fn blind_increment(&mut self, origin: String, value: f64) {
        assert!(value >= 0.0);
        let entry = self.increment_value.entry(origin).or_insert(0.0);
        *entry += value;
    }

    pub fn blind_decrement(&mut self, origin: String, value: f64) {
        assert!(value >= 0.0);
        let entry = self.decrement_value.entry(origin).or_insert(0.0);
        *entry += value;
    }  
}

impl PartialEq for PNCounterValue {
    fn eq(&self, other: &Self) -> bool {
        self.get_value() == other.get_value()
    }
}

impl Eq for PNCounterValue {}


impl WrongPNCounterValue {
    pub fn new() -> Self {
        Self {
            value: 0.0,
        }
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<f64>()
    }

    pub fn get_value(&self) -> f64 {
        self.value
    }

    pub fn merge(&mut self, new_value: Self) -> Result<f64, f64> {
        let mut any_change = false;
        let final_val = self.value.max(new_value.value);
        if (final_val - self.value) > 1e-6 {
            any_change = true;
        }
        self.value = final_val;
        // warn!("Merged PNCounterValue: {:?}", self);
        if any_change {
            Ok(self.get_value())
        } else {
            Err(self.get_value())
        }

    }

    pub fn merge_immutable(&self, new_value: &Self) -> Self {
        let mut val = self.clone();
        val.merge(new_value.clone());
        val
    }

    pub fn blind_increment(&mut self, origin: String, value: f64) {
        assert!(value >= 0.0);
        self.value += value;
    }

    pub fn blind_decrement(&mut self, origin: String, value: f64) {
        assert!(value >= 0.0);
        self.value -= value;
    }
}

impl PartialEq for WrongPNCounterValue {
    fn eq(&self, other: &Self) -> bool {
        self.get_value() == other.get_value()
    }
}

impl Eq for WrongPNCounterValue {}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum CachedValue {
    DWW(DWWValue),
    PNCounter(PNCounterValue),
}

impl CachedValue {
    pub fn is_dww(&self) -> bool {
        matches!(self, CachedValue::DWW(_))
    }

    pub fn is_pn_counter(&self) -> bool {
        matches!(self, CachedValue::PNCounter(_))
    }

    pub fn get_dww(&self) -> Option<&DWWValue> {
        if let CachedValue::DWW(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_pn_counter(&self) -> Option<&PNCounterValue> {
        if let CachedValue::PNCounter(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_dww_mut(&mut self) -> Option<&mut DWWValue> {
        if let CachedValue::DWW(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_pn_counter_mut(&mut self) -> Option<&mut PNCounterValue> {
        if let CachedValue::PNCounter(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn new_dww(value: Vec<u8>, val_hash: BigInt) -> Self {
        Self::DWW(DWWValue::new(value, val_hash))
    }

    pub fn new_dww_with_seq_num(value: Vec<u8>, seq_num: u64, val_hash: BigInt) -> Self {
        Self::DWW(DWWValue::new_with_seq_num(value, seq_num, val_hash))
    }

    pub fn new_pn_counter() -> Self {
        Self::PNCounter(PNCounterValue::new())
    }

    pub fn from_pn_counter(value: PNCounterValue) -> Self {
        Self::PNCounter(value)
    }

    pub fn size(&self) -> usize {
        match self {
            CachedValue::DWW(value) => value.size(),
            CachedValue::PNCounter(value) => value.size(),
        }
    }
}



pub type CacheKey = Vec<u8>;

pub struct CacheManager {
    config: AtomicPSLWorkerConfig,
    command_rx: tokio::sync::mpsc::Receiver<CacheCommand>,
    commit_command_rx: Receiver<CacheCommand>,
    block_rx: tokio::sync::mpsc::Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
    sequencer_request_rx: Receiver<TxWithAckChanTag>,
    
    block_sequencer_tx: Sender<SequencerCommand>,
    fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    cache: HashMap<CacheKey, CachedValue>,
    value_origin: HashMap<CacheKey, SenderType>,

    last_committed_seq_num: u64,
    batch_timer: Arc<Pin<Box<ResettableTimer>>>,
    last_batch_time: Instant,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    blocked_on_vc_wait: Option<oneshot::Receiver<()>>,
    block_on_read_snapshot: Option<oneshot::Receiver<()>>,


    #[cfg(feature = "evil")]
    __evil_weights: [(bool, f64); 2],
    #[cfg(feature = "evil")]
    __evil_dist: WeightedIndex<f64>,
    #[cfg(feature = "evil")]
    __evil_count: usize,
    #[cfg(feature = "evil")]
    __good_count: usize,
}

impl CacheManager {
    pub fn new(
        config: AtomicPSLWorkerConfig,
        command_rx: tokio::sync::mpsc::Receiver<CacheCommand>,
        commit_command_rx: Receiver<CacheCommand>,
        sequencer_request_rx: Receiver<TxWithAckChanTag>,
        block_rx: tokio::sync::mpsc::Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
        block_sequencer_tx: Sender<SequencerCommand>,
        fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    ) -> Self {
        let batch_timer = ResettableTimer::new(Duration::from_millis(config.get().worker_config.batch_max_delay_ms));
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        
        #[cfg(feature = "evil")]
        let __evil_weights = [(false, 1.0 - config.get().evil_config.rollbacked_response_ratio), (true, config.get().evil_config.rollbacked_response_ratio)];
        #[cfg(feature = "evil")]
        let __evil_dist = WeightedIndex::new(__evil_weights.iter().map(|(_, weight)| weight)).unwrap();

        Self {
            config,
            command_rx,
            commit_command_rx,
            sequencer_request_rx,
            block_rx,
            block_sequencer_tx,
            fork_receiver_cmd_tx,
            cache: HashMap::new(),
            value_origin: HashMap::new(),

            last_committed_seq_num: 0,
            batch_timer,
            last_batch_time: Instant::now(),
            log_timer,
            blocked_on_vc_wait: None,
            block_on_read_snapshot: None,

            #[cfg(feature = "evil")]
            __evil_weights,
            #[cfg(feature = "evil")]
            __evil_dist,
            #[cfg(feature = "evil")]
            __evil_count: 0,
            #[cfg(feature = "evil")]
            __good_count: 0,
        }
    }

    pub async fn run(cache_manager: Arc<Mutex<CacheManager>>) {
        let mut cache_manager = cache_manager.lock().await;

        cache_manager.batch_timer.run().await;
        cache_manager.log_timer.run().await;

        // File to log cache_manager stats
        while let Ok(_) = cache_manager.worker().await {
            // Handle errors if needed
        }
    }

    async fn check_block_on_vc_wait(blocked_on_vc_wait: &mut Option<oneshot::Receiver<()>>) -> Option<Result<(), RecvError>> {
        match blocked_on_vc_wait.as_mut() {
            Some(rx) => Some(rx.await),
            None => {
                std::future::pending::<()>().await;
                None
            },
        }
    }

    async fn check_block_on_read_snapshot(block_on_read_snapshot: &mut Option<oneshot::Receiver<()>>) -> Option<Result<(), RecvError>> {
        match block_on_read_snapshot.as_mut() {
            Some(rx) => Some(rx.await),
            None => {
                std::future::pending::<()>().await;
                None
            },
        }
    }

    async fn check_block_rx(block_rx: &mut tokio::sync::mpsc::Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, block_on_read_snapshot_is_some: bool) -> Option<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)> {
        if block_on_read_snapshot_is_some {
            std::future::pending::<()>().await;
            None
        } else {
            block_rx.recv().await
        }
    }

    async fn check_command_rx(command_rx: &mut tokio::sync::mpsc::Receiver<CacheCommand>, blocked_on_vc_wait_is_some: bool) -> Vec<CacheCommand> {
        if blocked_on_vc_wait_is_some {
            std::future::pending::<()>().await;
            Vec::new()
        } else {
            let mut commands = Vec::new();
            let _n = command_rx.len();
            if _n > 0 {
                command_rx.recv_many(&mut commands, _n).await;
            } else {
                if let Some(command) = command_rx.recv().await {
                    commands.push(command);
                }
            }
            commands
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {

        let block_on_vc_wait_is_some = self.blocked_on_vc_wait.is_some();
        let block_on_read_snapshot_is_some = if block_on_vc_wait_is_some { false } else { self.block_on_read_snapshot.is_some() };

        if block_on_vc_wait_is_some && block_on_read_snapshot_is_some {
            trace!("Both block_on_vc_wait and block_on_read_snapshot are set!!");
        }

        /*
        We only process new blocks from other workers if block_on_read_snapshot is None.
        Processing new blocks is the only way to clear block_on_vc_wait.

        We only process new commands if block_on_vc_wait is None.
        Processing new commands is the only way to clear block_on_read_snapshot.

        So, there is a chance of deadlock if both these blockers are active at the same time.
        However, in that case, the batch timer will fire, and a new block will be formed.
        (block_on_read_snapshot is set => there is at least one read op in the block sequencer => There will a new block when forced.)
        This new block clears block_on_read_snapshot, and that will eventually lead to enough new blocks from others to clear block_on_vc_wait.
        */

        tokio::select! {
            // biased;
            commands = Self::check_command_rx(&mut self.command_rx, block_on_vc_wait_is_some) => {
                if block_on_vc_wait_is_some {
                    error!("Command received while blocked on VC wait!!");
                }
                if commands.len() > 0 {
                    self.handle_command(commands).await;
                }
            },
            _ = self.batch_timer.wait() => {
                // This is safe to do here.
                // The tick won't interrupt handle_command or handle_block's logic.
                if self.last_batch_time.elapsed() > Duration::from_millis(self.config.get().worker_config.batch_max_delay_ms) {
                    self.last_batch_time = Instant::now();
                    let _ = self.block_sequencer_tx.send(SequencerCommand::ForceMakeNewBlock).await;
                    if block_on_vc_wait_is_some && block_on_read_snapshot_is_some {
                        error!("Force making new block under both block_on_vc_wait and block_on_read_snapshot");
                    }
                }
            },
            Some((block_rx, sender, _)) = Self::check_block_rx(&mut self.block_rx, block_on_read_snapshot_is_some) => {
                let block = block_rx.await.expect("Block rx error");
                if let Ok(block) = block {
                    self.handle_block(sender, block).await;
                } else {
                    warn!("Failed to get block from block_rx");
                }
            },

            Some(Ok(_)) = Self::check_block_on_vc_wait(&mut self.blocked_on_vc_wait) => {
                trace!("VC wait cleared");
                self.blocked_on_vc_wait = None;
            },
            Some(Ok(_)) = Self::check_block_on_read_snapshot(&mut self.block_on_read_snapshot) => {
                self.block_on_read_snapshot = None;
                trace!("Snapshot cleared");
            },
            

            Some((Some(tx), _)) = self.sequencer_request_rx.recv() => {
                self.handle_sequencer_request(tx).await;
            },
            // It is important that command_rx be checked before commit_command_rx.
            Some(cmd) = self.commit_command_rx.recv() => {
                if let CacheCommand::Commit(_, _) = cmd {
                    self.handle_command(vec![cmd]).await;
                } else {
                    error!("Unexpected commit command: {:?}", cmd);
                }
            },
            
            
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            },
        }
   
        Ok(())
    }

    async fn log_stats(&mut self) {
        let (max_seq_num, max2_seq_num, max_key, max2_key) = self.cache.iter()
        .filter(|(_, val)| val.is_dww())
        .fold((0u64, 0u64, CacheKey::new(), CacheKey::new()), |acc, (key, val)| {
            let val = val.get_dww().unwrap();
            if val.seq_num > acc.0 {
                (val.seq_num, acc.0, key.clone(), acc.2)
            } else if val.seq_num > acc.1 {
                (acc.0, val.seq_num, acc.2, key.clone())
            } else {
                acc
            }
        });

        info!("Cache size: {}, Max seq num: {} with Key: {}, Second max seq num: {} with Key: {}",
            self.cache.len(),
            max_seq_num, String::from_utf8(max_key.clone()).unwrap_or(hex::encode(max_key)),
            max2_seq_num, String::from_utf8(max2_key.clone()).unwrap_or(hex::encode(max2_key))
        );

        #[cfg(feature = "evil")]
        {
            info!("😈 Rolled back responses: {}", self.__evil_count);
            info!("😇 Good responses: {}", self.__good_count);
        }

    }

    fn maybe_respond_with_rolledback_state<'a>(&self, res: Option<&'a CachedValue>, key: &CacheKey) -> (Option<&'a CachedValue>, bool) {
        #[cfg(not(feature = "evil"))]
        return res;

        #[cfg(feature = "evil")]
        {
            use rand::thread_rng;

            let config = &self.config.get().evil_config;

            let should_respond_with_rolledback_state = if config.simulate_byzantine_behavior && config.rollbacked_response_ratio > 0.000000001 /* Avoid floating point errors */ {
                self.__evil_weights[self.__evil_dist.sample(&mut thread_rng())].0
            } else {
                false
            };
            if should_respond_with_rolledback_state {
                trace!("😈 Responding with rolledback state for key: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)));
                return (None, true);
            }
            (res, false)
        }
    }


    async fn _handle_command_single(&mut self, command: CacheCommand) {
        match command {
            CacheCommand::Get(key, should_block_snapshot, seq_num_query, response_tx) => {
                let res = self.cache.get(&key);

                #[cfg(feature = "evil")]
                let (res, did_rollback) = self.maybe_respond_with_rolledback_state(res, &key);

                #[cfg(feature = "evil")]
                {
                    if did_rollback {
                        self.__evil_count += 1;
                    } else {
                        self.__good_count += 1;
                    }
                }

                if res.is_none() {
                    trace!("Key not found: {:?}", key);
                }
                let origin = self.value_origin.get(&key).cloned().unwrap_or(SenderType::Auth("devil".to_string(), 0));
                let _ = response_tx.send(res.cloned().ok_or(CacheError::KeyNotFound));
                // let _ = response_tx.send(Err(CacheError::KeyNotFound));
                // let snapshot_propagated_signal_tx = if self.block_on_read_snapshot.is_some() || !should_block_snapshot {
                //     None
                // } else {
                //     let (tx, rx) = oneshot::channel();
                //     self.block_on_read_snapshot = Some(rx);
                //     trace!("Snapshot set");

                //     Some(tx)
                // };

                match seq_num_query {
                    BlockSeqNumQuery::WaitForSeqNum(tx) => {
                        let _ = tx.send(0);
                    }
                    _ => {}
                }


                // if should_block_snapshot == true:
                // On the first read op of the block, the snapshot is fixed.
                // Henceforth, all reads are based on this snapshot.
                // Until the block sequencer proposes the new block. After that, the snapshot can be updated.
                // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                // let _ = self.block_sequencer_tx.send(SequencerCommand::SelfReadOp { 
                //     key: key.clone(),
                //     value: res.map(|v| v.clone()),
                //     snapshot_propagated_signal_tx,
                //     origin,
                //     seq_num_query,
                //     // current_vc: current_vc_tx,
                // }).await;
                // let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key: key.clone(), value: res.cloned().unwrap_or(CachedValue::new_dww(vec![], BigInt::from_bytes_be(Sign::Plus, &hash(&key)))), seq_num_query, /* current_vc: current_vc_tx */ }).await;

                // let current_vc = current_vc_rx.await.unwrap();

                // trace!("Read key: {}, value_hash: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), hex::encode(cached_value_to_val_hash(res.cloned())));


            }
            CacheCommand::Put(key, value, seq_num_query, response_tx) => {
                self.value_origin.insert(key.clone(), SenderType::Auth(self.config.get().net_config.name.clone(), 0));
                assert!(value.is_dww());

                let value = value.get_dww().unwrap();
                if self.cache.contains_key(&key) {
                    let seq_num = self.cache.get_mut(&key).unwrap().get_dww_mut().unwrap().blind_update(value.value.clone(), value.val_hash.clone());
                    response_tx.send(Ok(seq_num)).unwrap();
                    // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                    let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key: key.clone(), value: CachedValue::new_dww_with_seq_num(value.value.clone(), seq_num, value.val_hash.clone()), seq_num_query, /* current_vc: current_vc_tx */ }).await;
                    // let current_vc = current_vc_rx.await.unwrap();
                    trace!("Write key: {}, value_hash: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), hex::encode(value.val_hash.to_bytes_be().1));
                    return;
                }

                let cached_value = CachedValue::new_dww(value.value.clone(), value.val_hash.clone());
                self.cache.insert(key.clone(), cached_value);
                response_tx.send(Ok(1)).unwrap();
                // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key: key.clone(), value: CachedValue::new_dww(value.value.clone(), value.val_hash.clone()), seq_num_query, /* current_vc: current_vc_tx */ }).await;
                // let current_vc = current_vc_rx.await.unwrap();
                trace!("Write key: {}, value_hash: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), hex::encode(value.val_hash.to_bytes_be().1));
            },

            CacheCommand::Increment(key, value, seq_num_query, response_tx) => {
                // self.value_origin.insert(key.clone(), SenderType::Auth(self.config.get().net_config.name.clone(), 0));
                // assert!(value.is_dww());

                // let value = value.get_dww().unwrap();
                assert!(value >= 0.0);
                let my_name = self.config.get().net_config.name.clone();
                if self.cache.contains_key(&key) {
                    let entry = self.cache.get_mut(&key).unwrap().get_pn_counter_mut().unwrap();
                    entry.blind_increment(my_name, value);
                    let final_value = entry.get_value();
                    response_tx.send(Ok(final_value as u64 /* It doesn't matter what the seq_num is here. */)).unwrap();
                    // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                    let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key: key.clone(), value: CachedValue::from_pn_counter(entry.clone()), seq_num_query, /* current_vc: current_vc_tx */ }).await;
                    // let current_vc = current_vc_rx.await.unwrap();
                    trace!("Write key: {}, value_increment: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), value);
                    return;
                }

                let mut cached_value = CachedValue::new_pn_counter();
                cached_value.get_pn_counter_mut().unwrap().blind_increment(my_name, value);

                self.cache.insert(key.clone(), cached_value.clone());
                response_tx.send(Ok(cached_value.get_pn_counter().unwrap().get_value() as u64 /* It doesn't matter what the seq_num is here. */)).unwrap();
                // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key: key.clone(), value: cached_value, seq_num_query, /* current_vc: current_vc_tx */ }).await;
                // let current_vc = current_vc_rx.await.unwrap();
                trace!("Write key: {}, value_increment: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), value);
            },
            CacheCommand::Decrement(key, value, seq_num_query, response_tx) => {
                assert!(value >= 0.0);
                let my_name = self.config.get().net_config.name.clone();
                if self.cache.contains_key(&key) {
                    let entry = self.cache.get_mut(&key).unwrap().get_pn_counter_mut().unwrap();
                    entry.blind_decrement(my_name, value);
                    let final_value = entry.get_value();
                    response_tx.send(Ok(final_value as u64 /* It doesn't matter what the seq_num is here. */)).unwrap();
                    // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                    let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key: key.clone(), value: CachedValue::from_pn_counter(entry.clone()), seq_num_query, /* current_vc: current_vc_tx */ }).await;
                    // let current_vc = current_vc_rx.await.unwrap();
                    trace!("Write key: {}, value_decrement: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), value);
                    return;
                }

                let mut cached_value = CachedValue::new_pn_counter();
                cached_value.get_pn_counter_mut().unwrap().blind_decrement(my_name, value);

                self.cache.insert(key.clone(), cached_value.clone());
                response_tx.send(Ok(cached_value.get_pn_counter().unwrap().get_value() as u64 /* It doesn't matter what the seq_num is here. */)).unwrap();
                // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key: key.clone(), value: cached_value, seq_num_query, /* current_vc: current_vc_tx */ }).await;
                // let current_vc = current_vc_rx.await.unwrap();
                trace!("Write key: {}, value_decrement: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), value);
            },
            CacheCommand::Cas(key, value, expected_seq_num, response_tx) => {
                unimplemented!();
            }
            CacheCommand::Commit(sender, force_prepare) => {
                let (tx, rx) = oneshot::channel();
                let _ = self.block_sequencer_tx.send(SequencerCommand::MakeNewBlock(tx, Some(sender), force_prepare)).await;
                let actually_did_prepare = rx.await.unwrap();
                if actually_did_prepare {
                    self.last_batch_time = Instant::now();
                }
            }
            CacheCommand::WaitForVC(vc, tx2) => {
                let (tx, rx) = oneshot::channel();
                self.blocked_on_vc_wait = Some(rx);
                let _ = self.block_sequencer_tx.send(SequencerCommand::WaitForVC(vc, tx, Some(tx2))).await;
            }

            CacheCommand::TransparentWaitForVC(vc, tx) => {
                let _ = self.block_sequencer_tx.send(SequencerCommand::WaitForVC(vc, tx, None)).await;
            }
            CacheCommand::ClearVC(vc) => {
                let _ = self.block_sequencer_tx.send(SequencerCommand::MakeNewBlockToPropagateVC(vc)).await;
            }
            CacheCommand::QueryVC(sender) => {
                let _ = self.block_sequencer_tx.send(SequencerCommand::QueryVC(sender)).await;
            }
        }
    }
    async fn handle_command(&mut self, commands: Vec<CacheCommand>) {
        for command in commands {
            self._handle_command_single(command).await;
        }
    }

    async fn handle_sequencer_request(&mut self, mut tx: ProtoTransaction) {
        trace!("Handling sequencer request: {:?}", tx);
        for op in tx.on_receive.as_mut().unwrap().ops.drain(..) {
            match op.op_type() {
                ProtoTransactionOpType::BlockIndefinitely => {
                    let (tx, rx) = oneshot::channel();
                    self.blocked_on_vc_wait = Some(rx);
                    let mut vc = VectorClock::new();
                    let me = self.config.get().net_config.name.clone();
                    vc.advance(SenderType::Auth(me, 0), u64::MAX /* sense of indefinitely */);
                    let _ = self.block_sequencer_tx.send(SequencerCommand::WaitForVC(vc, tx, None)).await;
                },

                ProtoTransactionOpType::Unblock => {
                    let mut vc = VectorClock::new();
                    let me = self.config.get().net_config.name.clone();
                    vc.advance(SenderType::Auth(me, 0), u64::MAX);
                    let _ = self.block_sequencer_tx.send(SequencerCommand::UnblockVC(vc)).await;
                }
                _ => {
                    warn!("Unsupported sequencer request op: {:?}", op);
                }
            }
        }
    }

    async fn handle_block(&mut self, sender: SenderType, block: CachedBlock) {
        let name = block.block.origin.clone();
        
        for tx in &block.block.tx_list {
            if tx.on_crash_commit.is_none() {
                continue;
            }

            let ops = tx.on_crash_commit.as_ref().unwrap();
            for op in &ops.ops {
                let Some((key, cached_value)) = process_tx_op(op) else { continue };

                // let cached_value = CachedValue {
                //     value,
                //     seq_num,

                // };

                // If this put request leads to an update,
                // It should be propagated downstream in the gossip/multicast tree,
                // As they may or may not receive the update directly.
                let (should_propagate, seq_num) = if self.cache.contains_key(&key) {
                    let val = self.cache.get_mut(&key).unwrap();
                    let res = match val {
                        CachedValue::DWW(dww_val) => {
                            dww_val.merge_cached(cached_value.get_dww().unwrap().clone())
                        },
                        CachedValue::PNCounter(pn_counter_val) => {
                            pn_counter_val.merge(cached_value.get_pn_counter().unwrap().clone())
                                .map(|_| 0u64).map_err(|_| 0u64)
                            // Ok(0) // The number doesn't matter here
                            // PN Counters must always be propagated.
                        }
                    };
                    match res {
                        Ok(_seq_num) => {
                            self.value_origin.insert(key.clone(), sender.clone());
                            (true, _seq_num)
                        },
                        Err(_old_seq_num) => (false, _old_seq_num)
                    }
                } else {
                    self.cache.insert(key.clone(), cached_value.clone());
                    self.value_origin.insert(key.clone(), sender.clone());
                    (true, 0)
                };

                // let should_propagate = true;

                #[cfg(not(feature = "gossip"))]
                let should_propagate = false;

                if should_propagate {
                    // let (current_vc_tx, current_vc_rx) = oneshot::channel();
                    let _ = self.block_sequencer_tx.send(SequencerCommand::OtherWriteOp {
                        key: key.clone(),
                        value: cached_value.clone(),
                        // current_vc: current_vc_tx,
                    }).await;
                    // let current_vc = current_vc_rx.await.unwrap();
                    trace!("Write key: {}, value_hash: {}", String::from_utf8(key.clone()).unwrap(), hex::encode(cached_value_to_val_hash(Some(cached_value))));
                } else {
                    trace!("Write rejected for key: {}, value_hash: {}, seq_num: {} origin: {}", String::from_utf8(key.clone()).unwrap(), hex::encode(cached_value_to_val_hash(Some(cached_value))), seq_num, sender.to_name_and_sub_id().0);
                }
            }
        }

        // Advance the vector clock in the sequencer.
        let mut advance_vc_commands = vec![];
        if block.block.vector_clock.is_some() {
            let vector_clock = block.block.vector_clock.as_ref().unwrap();
            for entry in vector_clock.entries.iter() {
                let sender = SenderType::Auth(entry.sender.clone(), 0);
                // let _ = self.block_sequencer_tx.send(SequencerCommand::AdvanceVC { sender, block_seq_num: entry.seq_num }).await;
                advance_vc_commands.push(AdvanceVCCommand { sender, block_seq_num: entry.seq_num });
            }
        }

        
        if !name.contains("god") {
            let block_seq_num = block.block.n;
            // let _ = self.block_sequencer_tx.send(SequencerCommand::AdvanceVC {
            //     sender: sender.clone(),
            //     block_seq_num,
            // }).await;
            advance_vc_commands.push(AdvanceVCCommand { sender: sender.clone(), block_seq_num });
            let _ = self.block_sequencer_tx.send(SequencerCommand::AdvanceVC(advance_vc_commands)).await;
        
            // A new block can be formed now.
            let (tx, rx) = oneshot::channel();
            let _ = self.block_sequencer_tx.send(SequencerCommand::MakeNewBlock(tx, None, false)).await;
            let actually_did_prepare = rx.await.unwrap();
            if actually_did_prepare {
                self.last_batch_time = Instant::now();
            }
            
            // Confirm the block to the fork receiver.
            let _ = self.fork_receiver_cmd_tx.send(ForkReceiverCommand::Confirm(sender, block_seq_num));
        } else {
            let _ = self.block_sequencer_tx.send(SequencerCommand::AdvanceVC(advance_vc_commands)).await;
        }

    }
}

pub fn process_tx_op(op: &ProtoTransactionOp) -> Option<(CacheKey, CachedValue)> {
    if op.op_type != ProtoTransactionOpType::Write as i32
    && op.op_type != ProtoTransactionOpType::Increment as i32
    {
        return None;
    }

    if op.operands.len() != 2 {
        return None;
    }

    let key = &op.operands[0];
    let value = &op.operands[1];
    
    let cached_value = bincode::deserialize::<CachedValue>(&value);
    if cached_value.is_err() {
        warn!("Failed to deserialize cached value: {:?}", value);
        return None;
    }
    let cached_value = cached_value.unwrap();

    Some((key.clone(), cached_value))
}

mod tests {
    use super::*;

    #[test]
    fn test_commutativity() {
        let val1 = DWWValue::new(b"not_important1".to_vec(),
            BigInt::from_bytes_be(Sign::Plus, &hex::decode("3fb43b0c4f06f6d6d3860f57f1040ab2c70594f5be33b9b2e0f1d14850b4f93f6f84f8901579f0cef45d83d19c048455eafae5e945553ac9db0cecb20f17aa24").unwrap()));
        let val2 = DWWValue::new(b"not_important2".to_vec(),
            BigInt::from_bytes_be(Sign::Plus, &hex::decode("e36e2d6085a127a4989d428f38df7e3a632bd5cef4a5216c76196909786ffb1badb575a5757af1ca4f0c8d97d8c6334701d85c9238fa5de297aaa62580491855").unwrap()));

        let mut test1 = val1.clone();
        let mut test2 = val2.clone();

        let _ = test1.merge_cached(val2.clone());
        let _ = test2.merge_cached(val1.clone());

        assert_eq!(test1.value, test2.value);
        assert_eq!(test1.seq_num, test2.seq_num);
        assert_eq!(test1.val_hash, test2.val_hash);

        assert_eq!(test1.val_hash, val2.val_hash);
        assert_eq!(test2.val_hash, val2.val_hash);
    }

    #[test]
    fn test_idempotence() {
        let mut val1 = PNCounterValue::new();
        val1.blind_decrement("foo".to_string(), 8.0);
        val1.blind_increment("bar".to_string(), 10.0);

        let _v1 = val1.get_value();

        let mut val2 = PNCounterValue::new();
        val2.blind_increment("foo".to_string(), 10.0);
        val2.blind_decrement("bar".to_string(), 8.0);

        let _v2 = val2.get_value();

        val1.merge(val2.clone());
        let _v3 = val1.get_value();
        val1.merge(val2);
        let _v4 = val1.get_value();


        assert_eq!(_v3, _v4);
        assert_eq!(_v1, 2.0);
        assert_eq!(_v2, 2.0);
        assert_eq!(_v3, 4.0);
    }
}