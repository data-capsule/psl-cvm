use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::Duration};

use anyhow::Ok;
use futures::{channel::oneshot, stream::FuturesUnordered, StreamExt};
use log::{error, info, warn};
use num_bigint::{BigInt, Sign};
use prost::{DecodeError, Message as _};
use tokio::{sync::Mutex, task::JoinSet};

use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, consensus::batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag}, crypto::{default_hash, hash, HashType}, proto::{client::{ProtoClientReply, ProtoTransactionReceipt}, execution::{ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType, ProtoTransactionResult}}, rpc::{server::LatencyProfile, PinnedMessage, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::block_sequencer::BlockSeqNumQuery};

use super::cache_manager::{CacheCommand, CacheError};

pub struct CacheConnector {
    cache_tx: Sender<CacheCommand>,
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
    pub fn new(cache_tx: Sender<CacheCommand>) -> Self {
        Self { cache_tx }
    }

    pub async fn dispatch_read_request(
        &self,
        key: Vec<u8>,
    ) -> anyhow::Result<(Vec<u8>, u64), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let command = CacheCommand::Get(key, tx);

        self.cache_tx.send(command).await;

        let result = rx.await.unwrap();

        result
    }

    pub async fn dispatch_write_request(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> anyhow::Result<(u64 /* lamport ts */, tokio::sync::oneshot::Receiver<u64 /* block seq num */>), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        // let val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&value));

        // TODO: Use the actual hash.
        let val_hash = BigInt::from(1);
        // let command = CacheCommand::Put(key, value, val_hash, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);
        
        // Short circuit for now.
        let command = CacheCommand::Put(key, value, val_hash, BlockSeqNumQuery::DontBother, response_tx);

        // self.cache_tx.send(command).await;
        // let result = response_rx.await.unwrap()?;
        // std::result::Result::Ok((result, rx))
        std::result::Result::Ok((1, rx))
    }

    pub async fn dispatch_commit_request(&self) {
        let command = CacheCommand::Commit;

        // self.cache_tx.send(command).await;
    }
}

pub type UncommittedResultSet = (Vec<ProtoTransactionOpResult>, MsgAckChanWithTag, Option<u64> /* Some(potential seq_num; wait till committed) | None(reply immediately) */);

pub trait ClientHandlerTask {
    fn new(cache_tx: Sender<CacheCommand>, id: usize) -> Self;
    fn get_cache_connector(&self) -> &CacheConnector;
    fn get_id(&self) -> usize;
    fn get_total_work(&self) -> usize; // Useful for throghput calculation.
    fn on_client_request(&mut self, request: TxWithAckChanTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> impl Future<Output = Result<(), anyhow::Error>> + Send + Sync;
}

pub struct PSLAppEngine<T: ClientHandlerTask> {
    config: AtomicPSLWorkerConfig,
    cache_tx: Sender<CacheCommand>,
    client_command_rx: Receiver<TxWithAckChanTag>,
    commit_tx_spawner: tokio::sync::broadcast::Sender<u64>,
    handles: JoinSet<()>,
    client_handler_phantom: PhantomData<T>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl<T: ClientHandlerTask + Send + Sync + 'static> PSLAppEngine<T> {
    pub fn new(config: AtomicPSLWorkerConfig, cache_tx: Sender<CacheCommand>, client_command_rx: Receiver<TxWithAckChanTag>, commit_tx_spawner: tokio::sync::broadcast::Sender<u64>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config,
            cache_tx,
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

        for id in 0..app.config.get().worker_config.num_worker_threads_per_worker {
            let cache_tx = app.cache_tx.clone();
            let _reply_tx = reply_tx.clone();
            let client_command_rx = app.client_command_rx.clone();
            let (total_work_tx, total_work_rx) = make_channel(_chan_depth);
            total_work_txs.push(total_work_tx);

            app.handles.spawn(async move {
                let mut handler_task = T::new(cache_tx, id);

                loop {
                    tokio::select! {
                        Some(command) = client_command_rx.recv() => {
                            handler_task.on_client_request(command, &_reply_tx).await;
                        }
                        Some(_tx) = total_work_rx.recv() => {
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

        loop {
            app.log_timer.wait().await;

            let mut total_work = 0;
            for tx in &total_work_txs {
                let (_tx, _rx) = tokio::sync::oneshot::channel();
                tx.send(_tx).await.unwrap();

                total_work += _rx.await.unwrap();
            }

            info!("Total requests processed: {}", total_work);
        }
        Ok(())
    }
}


pub struct KVSTask {
    cache_connector: CacheConnector,
    id: usize,
    total_work: usize,
}

impl ClientHandlerTask for KVSTask {
    fn new(cache_tx: Sender<CacheCommand>, id: usize) -> Self {
        Self {
            cache_connector: CacheConnector::new(cache_tx),
            id,
            total_work: 0,
        }
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

    #[allow(unreachable_code)]
    async fn on_client_request(&mut self, request: TxWithAckChanTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        let req = &request.0;
        let resp = &request.1;
        self.total_work += 1;

        // // Short circuit for now.
        // return self.reply_receipt(resp, vec![ProtoTransactionOpResult {
        //     success: true,
        //     values: vec![],
        // }], None, reply_handler_tx).await;
        
        
        if req.is_none() {
            return self.reply_invalid(resp, reply_handler_tx).await;
        }


        let req = req.as_ref().unwrap();
        if req.on_receive.is_none() {

            // For PSL, all transactions must be on_receive.
            // on_crash_commit and on_byz_commit are meaningless.
            return self.reply_invalid(resp, reply_handler_tx).await;
        }

        let on_receive = req.on_receive.as_ref().unwrap();


        if let std::result::Result::Ok((results, seq_num)) = self.execute_ops(on_receive.ops.as_ref()).await {
            return self.reply_receipt(resp, results, seq_num, reply_handler_tx).await;
        }

        self.reply_invalid(resp, reply_handler_tx).await
        

    }
}

impl KVSTask {
    async fn execute_ops(&self, ops: &Vec<ProtoTransactionOp>) -> Result<(Vec<ProtoTransactionOpResult>, Option<u64>), anyhow::Error> {
        let mut atleast_one_write = false;
        let mut last_write_index = 0;
        let mut highest_committed_block_seq_num_needed = 0;
        let mut block_seq_num_rx_vec = FuturesUnordered::new();
        let mut results = Vec::new();

        for (i, op) in ops.iter().enumerate() {
            let op_type: Result<ProtoTransactionOpType, DecodeError> = op.op_type.try_into();
            if let Err(e) = op_type {
                return Err(e.into());
            }

            match op_type.unwrap() {
                ProtoTransactionOpType::Write  => {
                    atleast_one_write = true;
                    last_write_index = i;
                },
                _ => {}
            }
        }

        for op in ops {
            let op_type: Result<ProtoTransactionOpType, DecodeError> = op.op_type.try_into();
            if let Err(e) = op_type {
                return Err(e.into());
            }

            match op_type.unwrap() {
                ProtoTransactionOpType::Write => {
                    let key = op.operands[0].clone();
                    let value = op.operands[1].clone();
                    let res = self.cache_connector.dispatch_write_request(key, value).await;
                    if let std::result::Result::Err(e) = res {
                        return Err(e.into());
                    }

                    let (_, block_seq_num_rx) = res.unwrap();
                    block_seq_num_rx_vec.push(block_seq_num_rx);
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
                }
                _ => {}
            }
            
        }

        self.cache_connector.dispatch_commit_request().await;

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

        Ok((results, None))
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