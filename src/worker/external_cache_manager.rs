use std::{pin::Pin, sync::Arc, time::{Duration, Instant}};

use hashbrown::HashMap;
use log::{error, info, warn};
use num_bigint::{BigInt, Sign};
use thiserror::Error;
use tokio::sync::{mpsc::UnboundedSender, oneshot, Mutex};
use crate::{config::AtomicPSLWorkerConfig, crypto::{hash, CachedBlock}, proto::execution::{ProtoTransactionOp, ProtoTransactionOpType}, rpc::SenderType, storage_server::fork_receiver::ForkReceiverCommand, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::{BlockSeqNumQuery, VectorClock}, cache_manager::{CacheCommand, CacheError, CacheKey, CachedValue}}};
use crate::worker::block_sequencer::SequencerCommand;

pub struct ExternalCacheManager {
    config: AtomicPSLWorkerConfig,
    command_rx: Receiver<CacheCommand>,
    block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
    block_sequencer_tx: Sender<SequencerCommand>,
    fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    cache: HashMap<CacheKey, CachedValue>,

    last_committed_seq_num: u64,
    last_batch_time: Instant,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    blocked_on_vc_wait: Option<oneshot::Receiver<()>>,
}


/// Forwards all read and write operations to a remote linearizable KV store.
/// Could be one of PirateShip's consensus protocols with a KV Store as an app.
/// Or Nimble's in memory store (with/without PSL storage backing it.) See: https://github.com/data-capsule/Nimble
impl ExternalCacheManager {
    pub fn new(
        config: AtomicPSLWorkerConfig,
        command_rx: Receiver<CacheCommand>,
        block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
        block_sequencer_tx: Sender<SequencerCommand>,
        fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    ) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        
        Self {
            config,
            command_rx,
            block_rx,
            block_sequencer_tx,
            fork_receiver_cmd_tx,
            cache: HashMap::new(),
            last_committed_seq_num: 0,
            last_batch_time: Instant::now(),
            log_timer,
            blocked_on_vc_wait: None,
        }
    }

    pub async fn run(cache_manager: Arc<Mutex<ExternalCacheManager>>) {
        let mut cache_manager = cache_manager.lock().await;

        cache_manager.log_timer.run().await;
        
        while let Ok(_) = cache_manager.worker().await {
            // Handle errors if needed
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        if self.blocked_on_vc_wait.is_some() {
            tokio::select! {
                Ok(_) = self.blocked_on_vc_wait.as_mut().unwrap() => {
                    self.blocked_on_vc_wait = None;
                }
                Some((block_rx, sender, _)) = self.block_rx.recv() => {
                    let block = block_rx.await.expect("Block rx error");
                    if let Ok(block) = block {
                        self.handle_block(sender, block).await;
                    } else {
                        warn!("Failed to get block from block_rx");
                    }
                }
                _ = self.log_timer.wait() => {
                    self.log_stats().await;
                }
            }
        } else {
            tokio::select! {
                Some(command) = self.command_rx.recv() => {
                    self.handle_command(command).await;
                }
                Some((block_rx, sender, _)) = self.block_rx.recv() => {
                    let block = block_rx.await.expect("Block rx error");
                    if let Ok(block) = block {
                        self.handle_block(sender, block).await;
                    } else {
                        warn!("Failed to get block from block_rx");
                    }
                }
                _ = self.log_timer.wait() => {
                    self.log_stats().await;
                }
            }
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
                #[cfg(feature = "external_cache_nimble")]
                self.get_from_nimble_kvs(key, response_tx).await;
                
                #[cfg(feature = "external_cache_pirateship")]
                self.get_from_pirateship_kvs(key, response_tx).await;
            }
            CacheCommand::Put(key, value, val_hash, seq_num_query, response_tx) => {
                #[cfg(feature = "external_cache_nimble")]
                self.put_to_nimble_kvs(key, value, val_hash, seq_num_query, response_tx).await;
                
                #[cfg(feature = "external_cache_pirateship")]
                self.put_to_pirateship_kvs(key, value, val_hash, seq_num_query, response_tx).await;
            }
            CacheCommand::Cas(key, value, expected_seq_num, response_tx) => {
                unimplemented!();
            }
            CacheCommand::Commit => {
                // The remote KV store is linearizable, but not serializable and has no sense of atomic multi-ops.
                // So we don't need to commit anything.
            }
            CacheCommand::WaitForVC(_) => {
                // This is No-op as well.
            }
        }
    }

    async fn handle_block(&mut self, sender: SenderType, block: CachedBlock) {
        // This does nothing in ExternalCacheManager.
        // Kept for API compatibility with CacheManager.
    }

    async fn get_from_nimble_kvs(&mut self, key: CacheKey, response_tx: oneshot::Sender<Result<(Vec<u8>, u64), CacheError>>) {
        unimplemented!();
    }

    async fn put_to_nimble_kvs(&mut self, key: CacheKey, value: Vec<u8>, val_hash: BigInt, seq_num_query: BlockSeqNumQuery, response_tx: oneshot::Sender<Result<u64, CacheError>>) {
        unimplemented!();
    }

    async fn get_from_pirateship_kvs(&mut self, key: CacheKey, response_tx: oneshot::Sender<Result<(Vec<u8>, u64), CacheError>>) {
        unimplemented!();
    }

    async fn put_to_pirateship_kvs(&mut self, key: CacheKey, value: Vec<u8>, val_hash: BigInt, seq_num_query: BlockSeqNumQuery, response_tx: oneshot::Sender<Result<u64, CacheError>>) {
        unimplemented!();
    }
}