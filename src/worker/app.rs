use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::Duration};

use anyhow::Ok;
use futures::{stream::FuturesUnordered, StreamExt};
use hashbrown::HashSet;
use itertools::Itertools;
use log::{error, info, trace, warn};
use nix::libc::sa_family_t;
use num_bigint::{BigInt, Sign};
use prost::Message as _;
use tokio::{sync::{mpsc::{UnboundedReceiver, UnboundedSender}, oneshot, Mutex}, task::JoinSet, time::Instant};

use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, consensus::batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag}, crypto::{default_hash, hash, AtomicKeyStore}, proto::{client::{ProtoClientReply, ProtoClientRequest, ProtoTransactionReceipt}, consensus::ProtoVectorClock, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType, ProtoTransactionPhase, ProtoTransactionResult}, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, server::LatencyProfile, PinnedMessage, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::{BlockSeqNumQuery, VectorClock}, cache_manager::CacheKey}};

use super::cache_manager::{CacheCommand, CacheError};

pub struct CacheConnector {
    cache_tx: tokio::sync::mpsc::Sender<CacheCommand>,
    cache_commit_tx: Sender<CacheCommand>,
    blocking_client: PinnedClient,
    client_tag_counter: u64,
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
    ) -> anyhow::Result<(Vec<u8>, u64), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let command = CacheCommand::Get(key.clone(), tx);

        self.cache_tx.send(command).await.unwrap();
        let result = rx.await.unwrap();

        result
    }

    pub async fn dispatch_write_request(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> anyhow::Result<(u64 /* lamport ts */, Option<tokio::sync::oneshot::Receiver<u64 /* block seq num */>>), CacheError> {
        // let (tx, rx) = tokio::sync::oneshot::channel();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&value));
        let command = CacheCommand::Put(key.clone(), value, val_hash, BlockSeqNumQuery::DontBother, response_tx);


        self.cache_tx.send(command).await.unwrap();

        let result = response_rx.await.unwrap()?;
        std::result::Result::Ok((result, None))
    }

    pub async fn dispatch_commit_request(&self, _force_prepare: bool) -> VectorClock {
        let (tx, rx) = oneshot::channel();
        let command = CacheCommand::Commit(tx, false /* force_prepare */);
        
        self.cache_tx.send(command).await;
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
            
            loop {
                let (tx, rx) = oneshot::channel();
                let _ = self.cache_tx.send(CacheCommand::QueryVC(tx)).await.unwrap();
                let curr_vc = rx.await.unwrap();
                if curr_vc >= vc {
                    break;
                }

                trace!("VC wait time: {:?} curr_vc: {} need vc: {}", start_time.elapsed(), curr_vc, vc);

                tokio::time::sleep(Duration::from_millis(1)).await;
            }

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
                            
                        },
                        Some(result) = _reply_rx.recv() => {
                            let (result, ack_chan, seq_num) = result;
                            let seq_num = seq_num.unwrap_or(0);
                            pending_results.push((result, ack_chan, seq_num));
                        }
                    }
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

                    }

                    pending_results.retain(|(_, _, seq_num)| *seq_num > commit_seq_num);
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


pub struct KVSTask {
    cache_connector: CacheConnector,
    id: usize,
    total_work: usize,

    locked_keys: Vec<(CacheKey, bool /* is_read */)>,

    once_lock: bool,
}

enum Response {
    Invalid(MsgAckChanWithTag),
    Receipt(MsgAckChanWithTag, Vec<ProtoTransactionOpResult>, Option<u64>),
}

impl ClientHandlerTask for KVSTask {
    fn new(cache_connector: CacheConnector, id: usize) -> Self {
        Self {
            cache_connector,
            id,
            total_work: 0,
            locked_keys: Vec::new(),
            once_lock: false,
        }
    }

    fn get_locked_keys(&self) -> Vec<CacheKey> {
        self.locked_keys.iter().map(|(key, _)| key.clone()).collect()
    }

    fn get_cache_connector(&self) -> &CacheConnector {
        &self.cache_connector
    }

    fn get_total_work(&self) -> usize {
        self.total_work
    }

    fn get_id(&self) -> usize {
        self.id
    }

    async fn on_client_request(&mut self, requests: Vec<TxWithAckChanTag>, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        let mut response_vec = Vec::new();

        let mut tx_phases_resp = Vec::new();

        for request in requests {

            let req = &request.0;
            let resp = &request.1;
            self.total_work += 1;
            
            
            if req.is_none() {
                response_vec.push(Response::Invalid(resp.clone()));
                continue;
            }
    
    
            let req = req.as_ref().unwrap();
            if req.on_receive.is_none() {
    
                // For PSL, all transactions must be on_receive.
                // on_crash_commit and on_byz_commit are meaningless.
                response_vec.push(Response::Invalid(resp.clone()));
                continue;
            }
    
            let on_receive = req.on_receive.as_ref().unwrap();

            self.buffer_lock_requests(on_receive.ops.as_ref()).await;

            tx_phases_resp.push((on_receive.clone(), resp.clone()));
        }

        // Conservative 2PL: Sort the locks.
        let mut locked_keys = self.locked_keys.drain(..)
            .collect::<HashSet<_>>() // Remove duplicates.
            .into_iter()
            .sorted_by_key(|(key, _)| key.clone()).collect::<Vec<_>>();

        if locked_keys.len() > 1 {
            error!("Locked keys size: {}", locked_keys.len());
        }


        let start_time = Instant::now();
        let mut glob_vc = VectorClock::new();
        for key in &locked_keys {
            let vc = self.cache_connector.dispatch_lock_request(&vec![key.clone()]).await;
            if let std::result::Result::Ok(vc) = vc {
                for (sender, seq_num) in vc.iter() {
                        glob_vc.advance(sender.clone(), *seq_num);
                }
            }
        }
        trace!("Lock request time: {:?}", start_time.elapsed());



        let start_time = Instant::now();

        let mut all_reads = true;

        for (on_receive, resp) in tx_phases_resp {
            // for _ in 0..99 {
            //     self.execute_ops(on_receive.ops.as_ref()).await;
            // }
    
            if let std::result::Result::Ok((results, seq_num, atleast_one_write)) = self.execute_ops(on_receive.ops.as_ref()).await {
                if atleast_one_write {
                    all_reads = false;
                }
                response_vec.push(Response::Receipt(resp, results, seq_num));
            } else {
                response_vec.push(Response::Invalid(resp));
            }
    
        }

        trace!("Execute ops time: {:?}", start_time.elapsed());

        // Group commit. Supposed to improve throughput.
        locked_keys.reverse();
        let start_time = Instant::now();
        let vc = self.cache_connector.dispatch_commit_request(locked_keys.len() > 0).await;
        trace!("Committed with VC: {} Locked keys: {:?}", vc,
            locked_keys.iter().map(|(key, _)| String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone()))).collect::<Vec<_>>());

        let vc = if all_reads {
            glob_vc
        } else {
            vc
        };
        self.cache_connector.dispatch_unlock_request(locked_keys.iter().map(|(key, _)| (key.clone(), vc.clone())).collect()).await;
        trace!("Commit and unlock time: {:?}", start_time.elapsed());

        let start_time = Instant::now();
        for response in response_vec {
            match response {
                Response::Invalid(resp) => {
                    self.reply_invalid(&resp, reply_handler_tx).await;
                }
                Response::Receipt(resp, results, seq_num) => {
                    self.reply_receipt(&resp, results, seq_num, reply_handler_tx).await;
                }
            }
        }
        trace!("Reply time: {:?}", start_time.elapsed());

        Ok(())

    }
}

impl KVSTask {
    async fn buffer_lock_requests(&mut self, ops: &Vec<ProtoTransactionOp>) {
        // return;
        let name = self.cache_connector.blocking_client.0.config.get().net_config.name.clone();

        for op in ops {
            let op_type = op.op_type();
            match op_type {
                // ProtoTransactionOpType::Read => {
                //     self.locked_keys.push((op.operands[0].clone(), false));
                // }
                ProtoTransactionOpType::Write => {
                    self.locked_keys.push((format!("write_{}", String::from_utf8(op.operands[0].clone()).unwrap_or(hex::encode(op.operands[0].clone()))).as_bytes().to_vec(), false));
                }
                _ => {}
            }
        }
    }

    async fn execute_ops(&mut self, ops: &Vec<ProtoTransactionOp>) -> Result<(Vec<ProtoTransactionOpResult>, Option<u64>, bool), anyhow::Error> {
        let mut atleast_one_write = false;
        let mut last_write_index = 0;
        let mut highest_committed_block_seq_num_needed = 0;
        let mut block_seq_num_rx_vec = FuturesUnordered::new();
        let mut results = Vec::new();

        for (i, op) in ops.iter().enumerate() {
            let op_type = op.op_type();

            match op_type {
                ProtoTransactionOpType::Write  => {
                    atleast_one_write = true;
                    last_write_index = i;
                },
                _ => {}
            }
        }

        for op in ops {
            let op_type = op.op_type();

            match op_type {
                ProtoTransactionOpType::Write => {
                    let key = op.operands[0].clone();
                    let value = op.operands[1].clone();
                    let res = self.cache_connector.dispatch_write_request(key, value).await;
                    if let std::result::Result::Err(e) = res {
                        return Err(e.into());
                    }

                    let (_, block_seq_num_rx) = res.unwrap();
                    if let Some(block_seq_num_rx) = block_seq_num_rx {
                        block_seq_num_rx_vec.push(block_seq_num_rx);
                    }
                    results.push(ProtoTransactionOpResult {
                        success: true,
                        values: vec![],
                    });
                },
                ProtoTransactionOpType::Read => {
                    let key = op.operands[0].clone();
                    match self.cache_connector.dispatch_read_request(key).await {
                        std::result::Result::Ok((value, seq_num)) => {
                            results.push(ProtoTransactionOpResult {
                                success: true,
                                values: vec![value, seq_num.to_be_bytes().to_vec()],
                            });
                        }
                        std::result::Result::Err(_e) => {
                            results.push(ProtoTransactionOpResult {
                                success: false,
                                values: vec![],
                            });
                        }
                    };
                },
                _ => {}
            }
            
        }

        // if atleast_one_write {

        //     // Find the highest block seq num needed.
        //     while let Some(seq_num) = block_seq_num_rx_vec.next().await {
        //         if seq_num.is_err() {
        //             continue;
        //         }

        //         let seq_num = seq_num.unwrap();
        //         highest_committed_block_seq_num_needed = std::cmp::max(highest_committed_block_seq_num_needed, seq_num);
        //     }

        //     return Ok((results, Some(highest_committed_block_seq_num_needed)));
        // }

        Ok((results, None, atleast_one_write))
    }

    async fn reply_receipt(&self, resp: &MsgAckChanWithTag, results: Vec<ProtoTransactionOpResult>, seq_num: Option<u64>, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        reply_handler_tx.send((results, resp.clone(), seq_num)).await;
        Ok(())
    }

    async fn reply_invalid(&self, resp: &MsgAckChanWithTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        // For now, just send a blank result.
        
        reply_handler_tx.send((vec![], resp.clone(), None)).await;
        Ok(())
    }


}