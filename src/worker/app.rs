use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::Duration};

use anyhow::Ok;

use log::{error, info, trace, warn};
use num_bigint::{BigInt, Sign};
use prost::Message as _;
use tokio::{sync::{mpsc::{UnboundedReceiver, UnboundedSender}, oneshot, Mutex}, task::JoinSet, time::Instant};

use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, consensus::batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag}, crypto::{default_hash, hash, AtomicKeyStore}, proto::{client::{ProtoClientReply, ProtoClientRequest, ProtoTransactionReceipt}, consensus::ProtoVectorClock, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType, ProtoTransactionPhase, ProtoTransactionResult}, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, server::LatencyProfile, PinnedMessage, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::{BlockSeqNumQuery, VectorClock}}, utils::types::{CacheKey, CachedValue}};

use super::cache_manager::{CacheCommand, CacheError};

pub struct CacheConnector {
    pub(crate) cache_tx: tokio::sync::mpsc::Sender<CacheCommand>,
    pub(crate) cache_commit_tx: Sender<CacheCommand>,
    pub(crate) blocking_client: PinnedClient,
    pub(crate) client_tag_counter: u64,
}

// const NUM_WORKER_THREADS: usize = 4;
// const NUM_REPLIER_THREADS: usize = 20;

enum FutureSeqNum {
    None,
    Immediate(u64),
    Future(oneshot::Receiver<u64>),
}

impl FutureSeqNum {
    pub fn new() -> Self {
        Self::None
    }

    pub async fn get_seq_num(&mut self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Immediate(seq_num) => Some(*seq_num),
            Self::Future(rx) => {
                let seq_num = rx.await.unwrap();
                *self = Self::Immediate(seq_num);
                Some(seq_num)
            }
        }
    }
}

impl CacheConnector {
    pub fn new(cache_tx: tokio::sync::mpsc::Sender<CacheCommand>, cache_commit_tx: Sender<CacheCommand>, blocking_client: PinnedClient) -> Self {
        Self { cache_tx, cache_commit_tx, blocking_client, client_tag_counter: 0 }
    }

    pub async fn dispatch_read_request(
        &self,
        key: Vec<u8>,
    ) -> (anyhow::Result<(Vec<u8>, u64), CacheError>, Option<tokio::sync::oneshot::Receiver<u64 /* block seq num */>>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (seq_num_tx, seq_num_rx) = tokio::sync::oneshot::channel();
        let seq_num_query = BlockSeqNumQuery::WaitForSeqNum(seq_num_tx);
        // let command = CacheCommand::Get(key.clone(), true, tx);
        let command = CacheCommand::Get(key.clone(), false, seq_num_query, tx);

        self.cache_tx.send(command).await.unwrap();
        let result = rx.await.unwrap();

        // if let std::result::Result::Ok(CachedValue::PNCounter(_)) = &result {
        //     return Err(CacheError::TypeMismatch);
        // }

        (result.map(|v| {
            match v {
                CachedValue::DWW(v) => (v.value, v.seq_num),
                CachedValue::PNCounter(v) => (v.get_value().to_be_bytes().to_vec(), 0),
            }
        }), Some(seq_num_rx))

        // match result {
        //     std::result::Result::Ok(v) => {
        //         match v {
        //             CachedValue::DWW(v) => {
        //                 Ok((v.value, v.seq_num, Some(seq_num_rx)))
        //             }
        //             CachedValue::PNCounter(v) => {
        //                 Ok((v.get_value().to_be_bytes().to_vec(), 0, Some(seq_num_rx)))
        //             }
        //         }
        //     }
        //     std::result::Result::Err(e) => {
        //         Err((e, Some(seq_num_rx)))
        //     }
        // }
    }

    pub async fn dispatch_counter_read_request(
        &self,
        key: Vec<u8>,
        should_block_snapshot: bool,
    ) -> (anyhow::Result<f64, CacheError>, Option<tokio::sync::oneshot::Receiver<u64 /* block seq num */>>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (seq_num_tx, seq_num_rx) = tokio::sync::oneshot::channel();
        let seq_num_query = BlockSeqNumQuery::WaitForSeqNum(seq_num_tx);
        let command = CacheCommand::Get(key.clone(), should_block_snapshot, seq_num_query, tx);

        self.cache_tx.send(command).await.unwrap();
        let result = rx.await.unwrap();

        if let std::result::Result::Ok(CachedValue::DWW(_)) = &result {
            return (Err(CacheError::TypeMismatch), Some(seq_num_rx));
        }

        (result.map(|v| {
            if let CachedValue::PNCounter(v) = v {
                v.get_value()
            } else {
                unreachable!()
            }
        }), Some(seq_num_rx))
    }

    pub async fn dispatch_write_request(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> anyhow::Result<(u64 /* lamport ts */, Option<tokio::sync::oneshot::Receiver<u64 /* block seq num */>>), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&value));
        let value = CachedValue::new_dww(value, val_hash);
        let command = CacheCommand::Put(key.clone(), value, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);


        error!("Sending put request for key: {:?}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone())));
        self.cache_tx.send(command).await.unwrap();
        error!("Put request sent for key: {:?}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone())));

        let result = response_rx.await.unwrap()?;
        error!("Put request received for key: {:?}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone())));
        std::result::Result::Ok((result, Some(rx)))
    }

    pub async fn dispatch_increment_request(
        &self,
        key: Vec<u8>,
        value: f64,
    ) -> anyhow::Result<(f64, Option<tokio::sync::oneshot::Receiver<u64 /* block seq num */>>), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();
        let command = CacheCommand::Increment(key, value, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);
        self.cache_tx.send(command).await.unwrap();
        let result = response_rx.await.unwrap()?;
        std::result::Result::Ok((result as f64, Some(rx)))
    }

    pub async fn dispatch_blind_increment_request(
        &self,
        key: Vec<u8>,
        value: f64,
    ) -> anyhow::Result<f64, CacheError> {
        let (response_tx, response_rx) = oneshot::channel();
        let command = CacheCommand::Increment(key, value, BlockSeqNumQuery::DontBother, response_tx);
        self.cache_tx.send(command).await.unwrap();
        let result = response_rx.await.unwrap()?;
        std::result::Result::Ok(result as f64)
    }

    pub async fn dispatch_decrement_request(
        &self,
        key: Vec<u8>,
        value: f64,
    ) -> anyhow::Result<(f64, Option<tokio::sync::oneshot::Receiver<u64 /* block seq num */>>), CacheError> {
        let (tx, rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();
        let command = CacheCommand::Decrement(key, value, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);
        self.cache_tx.send(command).await.unwrap();
        let result = response_rx.await.unwrap()?;
        std::result::Result::Ok((result as f64, Some(rx)))
    }

    pub async fn dispatch_commit_request(&self, _force_prepare: bool) -> VectorClock {
        let (tx, rx) = oneshot::channel();
        let command = CacheCommand::Commit(tx, false /* force_prepare */);
        
        self.cache_tx.send(command).await.unwrap();
        let vc = rx.await.unwrap();
        vc
    }

    pub async fn dispatch_lock_request(&mut self, key_and_is_read: &Vec<(CacheKey, bool)>) -> Result<VectorClock, CacheError> {
        // return std::result::Result::Ok(());
        if key_and_is_read.len() == 0 {
            return std::result::Result::Ok(VectorClock::new());
        }

        let mut ops = Vec::new();
        for (key, is_read) in key_and_is_read {
            let op_type = if *is_read {
                ProtoTransactionOpType::Read
            } else {
                ProtoTransactionOpType::Write
            };

            ops.push(ProtoTransactionOp {
                op_type: op_type as i32,
                operands: vec![key.clone()],
            });
        }

        let tx = ProtoTransaction {
            on_crash_commit: Some(ProtoTransactionPhase {
                ops,
            }),
            on_byzantine_commit: None,
            on_receive: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

       let origin = self.blocking_client.0.config.get().net_config.name.clone();
       self.client_tag_counter += 1;
       let client_tag = self.client_tag_counter;

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                tx: Some(tx),
                origin,
                sig: vec![0u8; 1],
                client_tag,
            }))
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        let start_time = Instant::now();
        let res = PinnedClient::send_and_await_reply(&self.blocking_client, &"sequencer1".to_string(), request.as_ref()).await;
        // std::result::Result::Ok(())
        trace!("Lock request time: {:?}", start_time.elapsed());


        if let Err(e) = res {
            return Err(CacheError::LockNotAcquirable);
        }

        let res = res.unwrap();
        
        let std::result::Result::Ok(receipt) = ProtoClientReply::decode(&res.as_ref().0.as_slice()[0..res.as_ref().1]) else {
            return Err(CacheError::InternalError);
        };

        let Some(receipt) = receipt.reply else {
            return Err(CacheError::InternalError);
        };

        let crate::proto::client::proto_client_reply::Reply::Receipt(receipt) = receipt else {
            return Err(CacheError::InternalError);
        };

        let Some(result) = receipt.results else {
            return Err(CacheError::InternalError);
        };

        let Some(result) = result.result.first() else {
            return Err(CacheError::InternalError);
        };


        if result.success {
            let Some(vc) = result.values.first() else {
                return Err(CacheError::InternalError);
            };

            let _sz = vc.len();

            let std::result::Result::Ok(proto_vc) = ProtoVectorClock::decode(&vc.as_slice()[0.._sz]) else {
                return Err(CacheError::InternalError);
            };

            let vc = VectorClock::from(Some(proto_vc));
            
            let start_time = Instant::now();
            
            // loop {
                let (tx, rx) = oneshot::channel();
                let _ = self.cache_tx.send(CacheCommand::TransparentWaitForVC(vc.clone(), tx)).await.unwrap();
                let _ = rx.await.unwrap();
                // if curr_vc >= vc {
                //     break;
                // }

                trace!("VC wait time: {:?} need vc: {}", start_time.elapsed(), vc);

                // tokio::time::sleep(Duration::from_millis(1)).await;
            // }

            // error!("VC wait time: {:?}", start_time.elapsed());

            std::result::Result::Ok(vc)
        } else {
            Err(CacheError::LockNotAcquirable)
        }

    }

    pub async fn dispatch_unlock_request(&mut self, mut keys_and_vcs: Vec<(CacheKey, VectorClock)>) {
        if keys_and_vcs.len() == 0 {
            return;
        }
        // return;
        
        let op_type = ProtoTransactionOpType::Unblock;
        let mut aggregate_vc = VectorClock::new();

        let _locks = keys_and_vcs.iter().map(|(key, _)| String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone()))).collect::<Vec<_>>();

        let tx = ProtoTransaction {
            on_crash_commit: Some(ProtoTransactionPhase {
                ops: keys_and_vcs.drain(..).map(|(key, vc)| {
                    vc.iter().for_each(|(sender, seq_num)| {
                        aggregate_vc.advance(sender.clone(), *seq_num);
                    });
                    
                    ProtoTransactionOp {
                        op_type: op_type as i32,
                        operands: vec![key, vc.serialize().encode_to_vec()],
                    }
                }).collect(),
            }),
            on_byzantine_commit: None,
            on_receive: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        let origin = self.blocking_client.0.config.get().net_config.name.clone();

        self.client_tag_counter += 1;
        let client_tag = self.client_tag_counter;

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                tx: Some(tx),
                origin,
                sig: vec![0u8; 1],
                client_tag,
            }))
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        // self.cache_tx.send(CacheCommand::ClearVC(aggregate_vc)).unwrap();

        let start_time = Instant::now();
        let err = PinnedClient::send_and_await_reply(&self.blocking_client, &"sequencer1".to_string(), request.as_ref()).await;
        trace!("Unlock request time: {:?} for keys: {:?}", start_time.elapsed(), _locks);
        if err.is_err() {
            error!("Failed to send unlock request: {:?}", err);
        }

    }
}

pub type UncommittedResultSet = (Vec<ProtoTransactionOpResult>, MsgAckChanWithTag, Option<u64> /* Some(potential seq_num; wait till committed) | None(reply immediately) */);

pub trait ClientHandlerTask {
    fn new(cache_tx: CacheConnector, id: usize) -> Self;
    fn get_cache_connector(&self) -> &CacheConnector;
    fn get_id(&self) -> usize;
    fn get_total_work(&self) -> usize; // Useful for throghput calculation.
    fn get_locked_keys(&self) -> Vec<CacheKey>;
    fn on_client_request(&mut self, request: Vec<TxWithAckChanTag>, reply_handler_tx: &Sender<UncommittedResultSet>) -> impl Future<Output = Result<(), anyhow::Error>> + Send + Sync;
}

pub struct PSLAppEngine<T: ClientHandlerTask> {
    config: AtomicPSLWorkerConfig,
    key_store: AtomicKeyStore,
    cache_tx: tokio::sync::mpsc::Sender<CacheCommand>,
    cache_commit_tx: Sender<CacheCommand>,
    client_command_rx: Receiver<TxWithAckChanTag>,
    commit_tx_spawner: tokio::sync::broadcast::Sender<u64>,
    handles: JoinSet<()>,
    client_handler_phantom: PhantomData<T>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl<T: ClientHandlerTask + Send + Sync + 'static> PSLAppEngine<T> {
    pub fn new(config: AtomicPSLWorkerConfig, key_store: AtomicKeyStore, cache_tx: tokio::sync::mpsc::Sender<CacheCommand>, cache_commit_tx: Sender<CacheCommand>, client_command_rx: Receiver<TxWithAckChanTag>, commit_tx_spawner: tokio::sync::broadcast::Sender<u64>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config,
            key_store,
            cache_tx,
            cache_commit_tx,
            client_command_rx,
            commit_tx_spawner,
            handles: JoinSet::new(),
            client_handler_phantom: PhantomData,
            log_timer,
        }
    }

    pub async fn run(app: Arc<Mutex<Self>>) -> anyhow::Result<()> {
        let mut app = app.lock().await;
        let _chan_depth = app.config.get().rpc_config.channel_depth as usize;

        // We are relying on the MPMC functionality of async-channel.
        // tokio channel won't work here.
        let (reply_tx, reply_rx) = make_channel(_chan_depth);

        let mut total_work_txs: Vec<crate::utils::channel::AsyncSenderWrapper<tokio::sync::oneshot::Sender<usize>>> = Vec::new();

        app.log_timer.run().await;

        let client_config = AtomicConfig::new(app.config.get().to_config());

        // let mut handler_txs = Vec::new();


        for id in 0..app.config.get().worker_config.num_worker_threads_per_worker {
            let blocking_client = Client::new_atomic(client_config.clone(), app.key_store.clone(), false, (id + 0xcafebabe) as u64).into();
            // let nonblocking_client = Client::new_atomic(client_config.clone(), app.key_store.clone(), true, (id + 200) as u64).into();
            let cache_tx = app.cache_tx.clone();
            let cache_commit_tx = app.cache_commit_tx.clone();
            let cache_connector = CacheConnector::new(cache_tx, cache_commit_tx, blocking_client);
            let _reply_tx = reply_tx.clone();
            let (total_work_tx, total_work_rx) = make_channel(_chan_depth);
            total_work_txs.push(total_work_tx);
            let handler_rx = app.client_command_rx.clone();
            


            app.handles.spawn(async move {
                let mut handler_task = T::new(cache_connector, id);

                loop {
                    tokio::select! {
                        biased;
                        Some(command) = handler_rx.recv() => {
                            handler_task.on_client_request(vec![command], &_reply_tx).await;
                        }
                        Some(_tx) = total_work_rx.recv() => {
                            trace!("Locked keys: {:?}", handler_task.get_locked_keys());
                            _tx.send(handler_task.get_total_work());
                        }
                    }
                }
            });
        }

        for _ in 0..app.config.get().worker_config.num_replier_threads_per_worker {
            let _reply_rx = reply_rx.clone();
            let mut _commit_rx = app.commit_tx_spawner.subscribe();

            app.handles.spawn(async move {
                let mut commit_seq_num = 0;
                let mut pending_results = Vec::new();

                loop {
                    tokio::select! {
                        std::result::Result::Ok(seq_num) = _commit_rx.recv() => {
                            commit_seq_num = seq_num;
                            error!("Commit sequence number: {}", seq_num);
                        },
                        Some(result) = _reply_rx.recv() => {
                            let (result, ack_chan, seq_num) = result;
                            let seq_num = seq_num.unwrap_or(0);
                            pending_results.push((result, ack_chan, seq_num));
                        }
                    }

                    let mut total_replies = 0;
                    for (result, ack_chan, seq_num) in &pending_results {
                        if *seq_num > commit_seq_num {
                            continue;
                        }

                        let reply = ProtoTransactionReceipt {
                            block_n: *seq_num,
                            tx_n: 0,
                            results: Some(ProtoTransactionResult {
                                result: result.clone(),
                            }),
                            await_byz_response: false,
                            byz_responses: vec![],
                            req_digest: default_hash(),
                        };

                        let reply = ProtoClientReply {
                            client_tag: ack_chan.1,
                            reply: Some(crate::proto::client::proto_client_reply::Reply::Receipt(reply)),
                        };

                        let buf = reply.encode_to_vec();
                        let len = buf.len();
                        let msg = PinnedMessage::from(buf, len, SenderType::Anon);
                        ack_chan.0.send((msg, LatencyProfile::new())).await;

                        total_replies += 1;

                    }

                    pending_results.retain(|(_, _, seq_num)| *seq_num > commit_seq_num);

                    trace!("Sent {} replies", total_replies);
                }  
            });
        }


        let cmd_rx = &mut app.client_command_rx;
        let mut __rr_cnt = 0;
        loop {
            // let cmd_len = cmd_rx.len();
            // if cmd_len > 0 {
            //     let mut cmds = Vec::new();
            //     cmd_rx.recv_many(&mut cmds, cmd_len).await;
                
            //     handler_txs[__rr_cnt % handler_txs.len()].send(cmds).await;
            //     __rr_cnt += 1;

            //     continue;
            // }
            // tokio::select! {
            //     biased;
            //     Some(cmd) = cmd_rx.recv() => {
            //         handler_txs[__rr_cnt % handler_txs.len()].send(vec![cmd]).await;
            //         __rr_cnt += 1;
            //     }
            //     // _ = app.log_timer.wait() => {
            //     //     let mut total_work = 0;
            //     //     for tx in &total_work_txs {
            //     //         let (_tx, _rx) = tokio::sync::oneshot::channel();
            //     //         tx.send(_tx).await.unwrap();

            //     //         total_work += _rx.await.unwrap();
            //     //     }

            //     //     info!("Total requests processed: {}", total_work);
            //     // }
            // }
            app.log_timer.wait().await;
        }
        Ok(())
    }
}
