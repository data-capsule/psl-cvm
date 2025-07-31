use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::{Duration, Instant}};

use anyhow::Ok;
use futures::{channel::oneshot, stream::FuturesUnordered, StreamExt};
use log::{error, info, trace, warn};
use num_bigint::{BigInt, Sign};
use prost::{DecodeError, Message as _};
use tokio::{sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, Mutex}, task::JoinSet};

use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, consensus::batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag}, crypto::{default_hash, hash, HashType}, proto::{client::{ProtoClientReply, ProtoTransactionReceipt}, execution::{ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType, ProtoTransactionResult}}, rpc::{server::LatencyProfile, PinnedMessage, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::block_sequencer::BlockSeqNumQuery};

use super::cache_manager::{CacheCommand, CacheError};

pub struct CacheConnector {
    cache_tx: flume::Sender<CacheCommand>,
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
    pub fn new(cache_tx: flume::Sender<CacheCommand>) -> Self {
        Self { cache_tx }
    }

    pub async fn dispatch_read_request(
        &self,
        key: Vec<u8>,
    ) -> anyhow::Result<(Vec<u8>, u64), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let command = CacheCommand::Get(key, tx);

        self.cache_tx.send(command); // .await;

        let result = rx.await.unwrap();

        result
    }

    pub async fn dispatch_write_request(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        // must_wait_for_seq_num: bool,
    ) -> anyhow::Result<(u64 /* lamport ts */, tokio::sync::oneshot::Receiver<u64 /* block seq num */>), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        // let val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&value));

        // TODO: Use the actual hash.
        let val_hash = BigInt::from(1);
        // let command = CacheCommand::Put(key, value, val_hash, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);
        // let command = CacheCommand::Put(key, vec![], val_hash, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);
        let command = CacheCommand::Put(key, vec![], val_hash, BlockSeqNumQuery::DontBother, response_tx, Instant::now());
        
        // Short circuit for now.
        // let command = CacheCommand::Put(key, value, val_hash, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);

        // let __cache_tx_time = Instant::now();
        self.cache_tx.send(command); // .await;
        // info!("Cache tx time: {} us", __cache_tx_time.elapsed().as_micros());
        let result = response_rx.await.unwrap()?;
        // let result = 1;
        // tx.send(0);
        std::result::Result::Ok((result, rx))
        // std::result::Result::Ok((1, rx))
    }

    pub async fn dispatch_commit_request(&self) {
        let command = CacheCommand::Commit;

        self.cache_tx.send(command); // .await;
    }
}

pub type UncommittedResultSet = (Vec<ProtoTransactionOpResult>, MsgAckChanWithTag, Option<u64> /* Some(potential seq_num; wait till committed) | None(reply immediately) */);

pub trait ClientHandlerTask {
    fn new(cache_tx: flume::Sender<CacheCommand>, id: usize) -> Self;
    fn get_cache_connector(&self) -> &CacheConnector;
    fn get_id(&self) -> usize;
    fn get_total_work(&self) -> usize; // Useful for throghput calculation.
    fn on_client_request(&mut self, request: TxWithAckChanTag, reply_handler_tx: &UnboundedSender<UncommittedResultSet>) -> impl Future<Output = Result<(), anyhow::Error>> + Send + Sync;
}

pub struct PSLAppEngine<T: ClientHandlerTask> {
    config: AtomicPSLWorkerConfig,
    cache_tx: flume::Sender<CacheCommand>,
    client_command_rx: async_channel::Receiver<TxWithAckChanTag>,
    commit_rx: UnboundedReceiver<u64>,
    handles: JoinSet<()>,
    client_handler_phantom: PhantomData<T>,
    pending_replies: HashMap<u64, Vec<((Vec<ProtoTransactionOpResult>, MsgAckChanWithTag, u64), Instant)>>,
    __reply_rr_cnt: usize,
    // log_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl<T: ClientHandlerTask + Send + Sync + 'static> PSLAppEngine<T> {
    pub fn new(config: AtomicPSLWorkerConfig, cache_tx: flume::Sender<CacheCommand>, client_command_rx: async_channel::Receiver<TxWithAckChanTag>, commit_rx: UnboundedReceiver<u64>) -> Self {
        Self {
            config,
            cache_tx,
            client_command_rx,
            commit_rx,
            handles: JoinSet::new(),
            client_handler_phantom: PhantomData,
            pending_replies: HashMap::new(),
            __reply_rr_cnt: 0,
            // log_timer,
        }
    }

    async fn send_reply(result: Vec<ProtoTransactionOpResult>, ack_chan: MsgAckChanWithTag, seq_num: u64) {
        let reply = ProtoTransactionReceipt {
            block_n: seq_num,
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
        let _ = ack_chan.0.send((msg, LatencyProfile::new())).await;

    }

    async fn client_reply_handler(mut reply_processor_rx: Receiver<Vec<((Vec<ProtoTransactionOpResult>, MsgAckChanWithTag, u64), Instant)>>) {
        while let Some(results) = reply_processor_rx.recv().await {
            for ((result, ack_chan, seq_num), start_time) in results {
                trace!("Reply latency: {} us", start_time.elapsed().as_micros());
                Self::send_reply(result, ack_chan, seq_num).await;
            }
        }
    }
    

    async fn forward_replies(&mut self, known_ci: u64, reply_tx_vec: &Vec<Sender<Vec<((Vec<ProtoTransactionOpResult>, MsgAckChanWithTag, u64), Instant)>>>) {
        let committed_seq_nums = self.pending_replies.keys()
            .filter(|seq_num| **seq_num <= known_ci)
            .map(|seq_num| *seq_num)
            .collect::<Vec<_>>();

        for seq_num in committed_seq_nums {
            let replies = self.pending_replies.remove(&seq_num).unwrap();
            let idx = self.__reply_rr_cnt;
            self.__reply_rr_cnt = (idx + 1) % reply_tx_vec.len();
            let _ = reply_tx_vec[idx].send(replies).await;
        }
    }

    pub async fn run(app: Arc<Mutex<Self>>) -> anyhow::Result<()> {
        let mut app = app.lock().await;
        let _chan_depth = app.config.get().rpc_config.channel_depth as usize;

        let (reply_tx, mut reply_rx) = unbounded_channel();

        let mut total_work_txs: Vec<Sender<tokio::sync::oneshot::Sender<usize>>> = Vec::new();

        //app.log_timer.run().await;
        let log_timer = ResettableTimer::new(Duration::from_millis(app.config.get().app_config.logger_stats_report_ms));
        log_timer.run().await;

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
                        biased;
                        std::result::Result::Ok(command) = client_command_rx.recv() => {
                            handler_task.on_client_request(command, &_reply_tx).await;
                        }
                        // Some(_tx) = total_work_rx.recv() => {
                        //     _tx.send(handler_task.get_total_work());
                        // }
                    }
                }
            });
        }

        let mut reply_tx_vec = Vec::new();

        for id in 0..app.config.get().worker_config.num_replier_threads_per_worker {
            let (_reply_tx, _reply_rx) = make_channel(_chan_depth);
            app.handles.spawn(Self::client_reply_handler(_reply_rx));
            reply_tx_vec.push(_reply_tx);
        }

        let mut ci_vec = Vec::new();
        let mut known_ci = 0;
        let mut reply_vec = Vec::new();
        
        loop {
            ci_vec.clear();
            reply_vec.clear();
            
            let msgs_pending_ci_vec = app.commit_rx.len();
            let msgs_pending_reply_vec = reply_rx.len();

            tokio::select! {
                _ = app.commit_rx.recv_many(&mut ci_vec, msgs_pending_ci_vec) => {
                    let max_ci = ci_vec.iter().max().unwrap_or(&0);

                    if *max_ci > known_ci {
                        known_ci = *max_ci;
                    }

                    app.forward_replies(known_ci, &reply_tx_vec).await;


                },
                _ = reply_rx.recv_many(&mut reply_vec, msgs_pending_reply_vec) => {
                    for (result, ack_chan, seq_num) in reply_vec.drain(..) {
                        let seq_num = seq_num.unwrap_or(0);
                        app.pending_replies.entry(seq_num)
                            .or_insert(Vec::new())
                            .push(((result, ack_chan, seq_num), Instant::now()));
                    }

                    app.forward_replies(known_ci, &reply_tx_vec).await;
                }
                // _ = log_timer.wait() => {
                //     let mut total_work = 0;
                //     for tx in &total_work_txs {
                //         let (_tx, _rx) = tokio::sync::oneshot::channel();
                //         tx.send(_tx).await.unwrap();

                //         total_work += _rx.await.unwrap();
                //     }
                //     info!("Total requests processed: {}", total_work);
                // }
            }
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
    fn new(cache_tx: flume::Sender<CacheCommand>, id: usize) -> Self {
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
    async fn on_client_request(&mut self, request: TxWithAckChanTag, reply_handler_tx: &UnboundedSender<UncommittedResultSet>) -> anyhow::Result<()> {
        let req = &request.0;
        let resp = request.1;
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

    #[allow(unreachable_code)]
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
                    // continue;

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

        if atleast_one_write {

            // Find the highest block seq num needed.
            // while let Some(seq_num) = block_seq_num_rx_vec.next().await {
            //     if seq_num.is_err() {
            //         continue;
            //     }

            //     let seq_num = seq_num.unwrap();
            //     highest_committed_block_seq_num_needed = std::cmp::max(highest_committed_block_seq_num_needed, seq_num);
            // }

            return Ok((results, Some(highest_committed_block_seq_num_needed)));
        }

        Ok((results, None))
    }

    async fn reply_receipt(&self, resp: MsgAckChanWithTag, results: Vec<ProtoTransactionOpResult>, seq_num: Option<u64>, reply_handler_tx: &UnboundedSender<UncommittedResultSet>) -> anyhow::Result<()> {
        reply_handler_tx.send((results, resp, seq_num));
        Ok(())
    }

    async fn reply_invalid(&self, resp: MsgAckChanWithTag, reply_handler_tx: &UnboundedSender<UncommittedResultSet>) -> anyhow::Result<()> {
        // For now, just send a blank result.
        
        reply_handler_tx.send((vec![], resp, None));
        Ok(())
    }


}