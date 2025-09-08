use std::{pin::Pin, sync::Arc, time::{Duration, Instant}};

use hashbrown::HashMap;
use log::{error, info, warn};
use num_bigint::{BigInt, Sign};
use thiserror::Error;
use tokio::sync::{mpsc::UnboundedSender, oneshot::{self, error::RecvError}, Mutex};
use crate::{config::AtomicPSLWorkerConfig, crypto::{hash, CachedBlock}, proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType}, rpc::SenderType, storage_server::fork_receiver::ForkReceiverCommand, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::{BlockSeqNumQuery, VectorClock}, TxWithAckChanTag}};
use crate::worker::block_sequencer::SequencerCommand;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Internal error")]
    InternalError,


}

pub enum CacheCommand {
    Get(CacheKey /* Key */, oneshot::Sender<Result<(Vec<u8>, u64) /* Value, seq_num */, CacheError>>),
    Put(
        CacheKey /* Key */,
        Vec<u8> /* Value */,
        BigInt /* Val Hash */,
        BlockSeqNumQuery,
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    ),
    Cas(
        CacheKey /* Key */,
        Vec<u8> /* Value */,
        u64 /* Expected SeqNum */,
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    ),
    Commit,
    // WaitForVC(VectorClock),
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
    ) -> anyhow::Result<(Vec<u8>, u64)> {
        let res = dispatch!(self, CacheCommand::Get, key);
        Ok(res)
    }

    pub async fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        val_hash: BigInt,
        seq_num_query: BlockSeqNumQuery,
    ) -> anyhow::Result<()> {
        dispatch!(self, CacheCommand::Put, key, value, val_hash, seq_num_query);
        Ok(())
    }


}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct CachedValue {
    pub(crate) value: Vec<u8>,
    pub(crate) seq_num: u64,
    pub(crate) val_hash: BigInt,
}

impl CachedValue {

    pub fn get_value(&self) -> Vec<u8> {
        self.value.clone()
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
            self.value.copy_from_slice(&new_value);
            self.seq_num = new_seq_num;
            self.val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&self.value));
            return Ok(new_seq_num);
        } else if new_seq_num == self.seq_num {
            let new_hash = hash(&new_value);
            let new_hash_num = BigInt::from_bytes_be(Sign::Plus, &new_hash);
            if new_hash_num > self.val_hash {
                self.value.copy_from_slice(&new_value);
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
        new_value: CachedValue,
    ) -> Result<u64, u64> {
        if new_value.seq_num > self.seq_num {
            self.value.copy_from_slice(&new_value.value);
            self.seq_num = new_value.seq_num;
            self.val_hash = new_value.val_hash;
            return Ok(self.seq_num);
        } else if new_value.seq_num == self.seq_num {
            if new_value.val_hash > self.val_hash {
                self.value.copy_from_slice(&new_value.value);
                self.val_hash = new_value.val_hash;
                self.seq_num = new_value.seq_num;
                return Ok(self.seq_num);
            }
        }

        Err(self.seq_num)
    }

    pub fn merge_immutable(&self, new_value: &CachedValue) -> CachedValue {
        let mut val = self.clone();
        val.merge_cached(new_value.clone()).unwrap();
        val
    }
}

pub type CacheKey = Vec<u8>;

pub struct CacheManager {
    config: AtomicPSLWorkerConfig,
    command_rx: Receiver<CacheCommand>,
    block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
    sequencer_request_rx: Receiver<TxWithAckChanTag>,
    
    block_sequencer_tx: Sender<SequencerCommand>,
    fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    cache: HashMap<CacheKey, CachedValue>,

    last_committed_seq_num: u64,
    batch_timer: Arc<Pin<Box<ResettableTimer>>>,
    last_batch_time: Instant,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    blocked_on_vc_wait: Option<oneshot::Receiver<()>>,
    block_on_read_snapshot: Option<oneshot::Receiver<()>>,
}

impl CacheManager {
    pub fn new(
        config: AtomicPSLWorkerConfig,
        command_rx: Receiver<CacheCommand>,
        sequencer_request_rx: Receiver<TxWithAckChanTag>,
        block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
        block_sequencer_tx: Sender<SequencerCommand>,
        fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    ) -> Self {
        let batch_timer = ResettableTimer::new(Duration::from_millis(config.get().worker_config.batch_max_delay_ms));
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        
        Self {
            config,
            command_rx,
            sequencer_request_rx,
            block_rx,
            block_sequencer_tx,
            fork_receiver_cmd_tx,
            cache: HashMap::new(),
            last_committed_seq_num: 0,
            batch_timer,
            last_batch_time: Instant::now(),
            log_timer,
            blocked_on_vc_wait: None,
            block_on_read_snapshot: None,
        }
    }

    pub async fn run(cache_manager: Arc<Mutex<CacheManager>>) {
        let mut cache_manager = cache_manager.lock().await;

        cache_manager.batch_timer.run().await;
        cache_manager.log_timer.run().await;
        
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

    async fn check_block_rx(block_rx: &mut Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, block_on_read_snapshot_is_some: bool) -> Option<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)> {
        if block_on_read_snapshot_is_some {
            std::future::pending::<()>().await;
            None
        } else {
            block_rx.recv().await
        }
    }

    async fn check_command_rx(command_rx: &mut Receiver<CacheCommand>, blocked_on_vc_wait_is_some: bool) -> Option<CacheCommand> {
        if blocked_on_vc_wait_is_some {
            std::future::pending::<()>().await;
            None
        } else {
            command_rx.recv().await
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {

        let block_on_vc_wait_is_some = self.blocked_on_vc_wait.is_some();
        let block_on_read_snapshot_is_some = self.block_on_read_snapshot.is_some();

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
            biased;
            Some((block_rx, sender, _)) = Self::check_block_rx(&mut self.block_rx, block_on_read_snapshot_is_some) => {
                let block = block_rx.await.expect("Block rx error");
                if let Ok(block) = block {
                    self.handle_block(sender, block).await;
                } else {
                    warn!("Failed to get block from block_rx");
                }
            },

            Some(Ok(_)) = Self::check_block_on_vc_wait(&mut self.blocked_on_vc_wait) => {
                warn!("VC wait cleared");
                self.blocked_on_vc_wait = None;
            },
            Some(Ok(_)) = Self::check_block_on_read_snapshot(&mut self.block_on_read_snapshot) => {
                self.block_on_read_snapshot = None;
                // warn!("Snapshot cleared");
            },
            Some(command) = Self::check_command_rx(&mut self.command_rx, block_on_vc_wait_is_some) => {
                if block_on_vc_wait_is_some {
                    error!("Command received while blocked on VC wait!!");
                }
                self.handle_command(command).await;
            },

            Some((Some(tx), _)) = self.sequencer_request_rx.recv() => {
                self.handle_sequencer_request(tx).await;
            },
            
            _ = self.batch_timer.wait() => {
                // This is safe to do here.
                // The tick won't interrupt handle_command or handle_block's logic.
                if self.last_batch_time.elapsed() > Duration::from_millis(self.config.get().worker_config.batch_max_delay_ms) {
                    self.last_batch_time = Instant::now();
                    let _ = self.block_sequencer_tx.send(SequencerCommand::ForceMakeNewBlock).await;
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
        .fold((0u64, 0u64, CacheKey::new(), CacheKey::new()), |acc, (key, val)| {
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
    }

    async fn handle_command(&mut self, command: CacheCommand) {
        match command {
            CacheCommand::Get(key, response_tx) => {
                let res = self.cache.get(&key);
                let _ = response_tx.send(res.map(|v| (v.value.clone(), v.seq_num)).ok_or(CacheError::KeyNotFound));

                let snapshot_propagated_signal_tx = if self.block_on_read_snapshot.is_some() {
                    None
                } else {
                    let (tx, rx) = oneshot::channel();
                    self.block_on_read_snapshot = Some(rx);
                    Some(tx)
                };

                // On the first read op of the block, the snapshot is fixed.
                // Henceforth, all reads are based on this snapshot.
                // Until the block sequencer proposes the new block. After that, the snapshot can be updated.
                let _ = self.block_sequencer_tx.send(SequencerCommand::SelfReadOp { 
                    key,
                    value: res.map(|v| v.clone()),
                    snapshot_propagated_signal_tx
                }).await;
                // warn!("Snapshot set");

                // TODO: Fill from checkpoint if key not found.
            }
            CacheCommand::Put(key, value, val_hash, seq_num_query, response_tx) => {
                if self.cache.contains_key(&key) {
                    let seq_num = self.cache.get_mut(&key).unwrap().blind_update(value.clone(), val_hash.clone());
                    response_tx.send(Ok(seq_num)).unwrap();
                    
                    let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key, value: CachedValue::new_with_seq_num(value, seq_num, val_hash), seq_num_query }).await;
                    return;
                }

                let cached_value = CachedValue::new(value.clone(), val_hash.clone());
                self.cache.insert(key.clone(), cached_value);
                response_tx.send(Ok(1)).unwrap();
                let _ = self.block_sequencer_tx.send(SequencerCommand::SelfWriteOp { key, value: CachedValue::new(value, val_hash), seq_num_query }).await;

            }
            CacheCommand::Cas(key, value, expected_seq_num, response_tx) => {
                unimplemented!();
            }
            CacheCommand::Commit => {
                self.last_batch_time = Instant::now();
                let _ = self.block_sequencer_tx.send(SequencerCommand::MakeNewBlock).await;
            }
            // CacheCommand::WaitForVC(vc) => {
            //     let (tx, rx) = oneshot::channel();
            //     self.blocked_on_vc_wait = Some(rx);
            //     let _ = self.block_sequencer_tx.send(SequencerCommand::WaitForVC(vc, tx)).await;
            // }
        }
    }

    async fn handle_sequencer_request(&mut self, mut tx: ProtoTransaction) {
        error!("Handling sequencer request: {:?}", tx);
        for op in tx.on_receive.as_mut().unwrap().ops.drain(..) {
            match op.op_type() {
                ProtoTransactionOpType::BlockIndefinitely => {
                    let (tx, rx) = oneshot::channel();
                    self.blocked_on_vc_wait = Some(rx);
                    let mut vc = VectorClock::new();
                    let me = self.config.get().net_config.name.clone();
                    vc.advance(SenderType::Auth(me, 0), u64::MAX /* sense of indefinitely */);
                    let _ = self.block_sequencer_tx.send(SequencerCommand::WaitForVC(vc, tx)).await;
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
        let (name, _) = sender.to_name_and_sub_id();
        if name.contains("god") {
            error!("Received god block");
        }
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
                    let res = self.cache.get_mut(&key).unwrap().merge_cached(cached_value.clone());
                    match res {
                        Ok(seq_num) => (true, seq_num),
                        Err(_old_seq_num) => (false, 0 /* doesn't matter */)
                    }
                } else {
                    let seq_num = cached_value.seq_num;
                    self.cache.insert(key.clone(), cached_value.clone());
                    (true, seq_num)
                };

                #[cfg(not(feature = "gossip"))]
                let should_propagate = false;

                if should_propagate {
                    let _ = self.block_sequencer_tx.send(SequencerCommand::OtherWriteOp {
                        key: key.clone(),
                        value: cached_value,
                    }).await;
                }
            }
        }

        // Advance the vector clock in the sequencer.
        if block.block.vector_clock.is_some() {
            let vector_clock = block.block.vector_clock.as_ref().unwrap();
            for entry in vector_clock.entries.iter() {
                let sender = SenderType::Auth(entry.sender.clone(), 0);
                let _ = self.block_sequencer_tx.send(SequencerCommand::AdvanceVC { sender, block_seq_num: entry.seq_num }).await;
            }
        }

        let block_seq_num = block.block.n;
        let _ = self.block_sequencer_tx.send(SequencerCommand::AdvanceVC {
            sender: sender.clone(),
            block_seq_num,
        }).await;


        // A new block can be formed now.
        self.last_batch_time = Instant::now();
        let _ = self.block_sequencer_tx.send(SequencerCommand::MakeNewBlock).await;

        // Confirm the block to the fork receiver.
        let _ = self.fork_receiver_cmd_tx.send(ForkReceiverCommand::Confirm(sender, block_seq_num));

    }
}

pub fn process_tx_op(op: &ProtoTransactionOp) -> Option<(CacheKey, CachedValue)> {
    if op.op_type != ProtoTransactionOpType::Write as i32 {
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