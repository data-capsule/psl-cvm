use std::{pin::Pin, sync::Arc, time::{Duration, Instant}};

use hashbrown::HashMap;
use log::{error, info, warn};
use num_bigint::{BigInt, Sign};
use thiserror::Error;
use tokio::sync::{mpsc::UnboundedSender, oneshot, Mutex};
use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, crypto::{hash, AtomicKeyStore, CachedBlock}, proto::{client::{ProtoClientReply, ProtoClientRequest}, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase}, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, PinnedMessage, SenderType}, storage_server::fork_receiver::ForkReceiverCommand, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::{BlockSeqNumQuery, VectorClock}, cache_manager::{CacheCommand, CacheError, CacheKey, CachedValue}}};
use crate::worker::block_sequencer::SequencerCommand;

use prost::Message as _;

pub struct ExternalCacheManager {
    config: AtomicPSLWorkerConfig,
    key_store: AtomicKeyStore,
    client: PinnedClient,
    command_rx: Receiver<CacheCommand>,
    block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
    block_sequencer_tx: Sender<SequencerCommand>,
    fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    cache: HashMap<CacheKey, CachedValue>,

    last_committed_seq_num: u64,
    last_batch_time: Instant,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    blocked_on_vc_wait: Option<oneshot::Receiver<()>>,
    client_tag_counter: u64,
}


/// Forwards all read and write operations to a remote linearizable KV store.
/// Could be one of PirateShip's consensus protocols with a KV Store as an app.
/// Or Nimble's in memory store (with/without PSL storage backing it.) See: https://github.com/data-capsule/Nimble
impl ExternalCacheManager {
    pub fn new(
        config: AtomicPSLWorkerConfig,
        key_store: AtomicKeyStore,
        command_rx: Receiver<CacheCommand>,
        block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, std::io::Error>>, SenderType /* sender */, SenderType /* origin */)>, // Invariant for CacheManager: sender == origin
        block_sequencer_tx: Sender<SequencerCommand>,
        fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
    ) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        
        // This client need to behave exactly like a YCSB client.
        let client = Client::new_atomic(AtomicConfig::new(config.get().to_config()), key_store.clone(), true, 0).into();
        Self {
            config,
            key_store,
            client,
            command_rx,
            block_rx,
            block_sequencer_tx,
            fork_receiver_cmd_tx,
            cache: HashMap::new(),
            last_committed_seq_num: 0,
            last_batch_time: Instant::now(),
            log_timer,
            blocked_on_vc_wait: None,
            client_tag_counter: 0,
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
                self.get_from_external_kvs(key, response_tx).await;
            }
            CacheCommand::Put(key, value, val_hash, seq_num_query, response_tx) => {
                self.put_to_external_kvs(key, value, val_hash, seq_num_query, response_tx).await;
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

    async fn get_from_external_kvs(&mut self, key: CacheKey, response_tx: oneshot::Sender<Result<(Vec<u8>, u64), CacheError>>) {
        let tx = ProtoTransaction {
            on_receive: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Read as i32,
                    operands: vec![key],
                }],
            }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        let my_name = self.config.get().net_config.name.clone();
        let client_tag = self.client_tag_counter;
        self.client_tag_counter += 1;

        let request = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                tx: Some(tx),
                origin: my_name.clone(),
                sig: vec![0u8; 1],
                client_tag,
            }))
        };

        let buf = request.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        let remote_kvs_name = String::from("sequencer1"); // TODO: Make this configurable.
        let res = PinnedClient::send_and_await_reply(&self.client, &remote_kvs_name, request.as_ref()).await;

        if let Err(e) = res {
            error!("Failed to send request to remote KVS: {:?}", e);
            response_tx.send(Err(CacheError::InternalError));
            return;
        }
        
        let res = res.unwrap();
        let decoded_payload = match ProtoClientReply::decode(&res.as_ref().0.as_slice()[0..res.as_ref().1]) {
            Ok(payload) => payload,
            Err(e) => {
                error!("Failed to decode response: {:?}", e);
                response_tx.send(Err(CacheError::InternalError));
                return;
            }
        };

        let Some(reply) = decoded_payload.reply else {
            error!("Reply is None");
            response_tx.send(Err(CacheError::InternalError));
            return;
        };

        match reply {
            crate::proto::client::proto_client_reply::Reply::Receipt(reply) => {
                let Some(result) = reply.results else {
                    error!("Results is None");
                    response_tx.send(Err(CacheError::InternalError));
                    return;
                };

                if result.result.is_empty() {
                    error!("Result is empty");
                    response_tx.send(Err(CacheError::InternalError));
                    return;
                }

                let result = &result.result[0];
                if result.success {
                    response_tx.send(Ok((result.values[0].clone(), 0 /* this is unused here */)));
                } else {
                    response_tx.send(Err(CacheError::KeyNotFound));
                }
            },
            _ => {
                error!("Reply is not Receipt");
                response_tx.send(Err(CacheError::InternalError));
                return;
            }
        }
        
        


    }

    async fn put_to_external_kvs(&mut self, key: CacheKey, value: Vec<u8>, val_hash: BigInt, seq_num_query: BlockSeqNumQuery, response_tx: oneshot::Sender<Result<u64, CacheError>>) {
        let tx = ProtoTransaction {
            on_receive: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Write as i32,
                    operands: vec![key, value],
                }],
            }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        let my_name = self.config.get().net_config.name.clone();
        let client_tag = self.client_tag_counter;
        self.client_tag_counter += 1;

        let request = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                tx: Some(tx),
                origin: my_name.clone(),
                sig: vec![0u8; 1],
                client_tag,
            }))
        };

        let buf = request.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        let remote_kvs_name = String::from("sequencer1"); // TODO: Make this configurable.
        let res = PinnedClient::send_and_await_reply(&self.client, &remote_kvs_name, request.as_ref()).await;

        if let Err(e) = res {
            error!("Failed to send request to remote KVS: {:?}", e);
            response_tx.send(Err(CacheError::InternalError));
            return;
        }
        
        let res = res.unwrap();
        let decoded_payload = match ProtoClientReply::decode(&res.as_ref().0.as_slice()[0..res.as_ref().1]) {
            Ok(payload) => payload,
            Err(e) => {
                error!("Failed to decode response: {:?}", e);
                response_tx.send(Err(CacheError::InternalError));
                return;
            }
        };

        let Some(reply) = decoded_payload.reply else {
            error!("Reply is None");
            response_tx.send(Err(CacheError::InternalError));
            return;
        };

        match reply {
            crate::proto::client::proto_client_reply::Reply::Receipt(reply) => {
                let Some(result) = reply.results else {
                    error!("Results is None");
                    response_tx.send(Err(CacheError::InternalError));
                    return;
                };

                if result.result.is_empty() {
                    error!("Result is empty");
                    response_tx.send(Err(CacheError::InternalError));
                    return;
                }

                let result = &result.result[0];
                if result.success {
                    response_tx.send(Ok(0 /* this is unused here */));
                } else {
                    response_tx.send(Err(CacheError::InternalError));
                }
            },
            _ => {
                error!("Reply is not Receipt");
                response_tx.send(Err(CacheError::InternalError));
                return;
            }
        }
    }
}