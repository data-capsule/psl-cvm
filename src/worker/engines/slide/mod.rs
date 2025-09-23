// @Nurzhan: Add all SLIDE code in this folder.

use std::{collections::HashSet, time::Instant};

use futures::{stream::FuturesUnordered, StreamExt as _};
use itertools::Itertools;
use log::{error, trace};

use crate::{config::AtomicPSLWorkerConfig, consensus::batch_proposal::MsgAckChanWithTag, proto::execution::{ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType}, utils::channel::Sender, worker::{app::{CacheConnector, ClientHandlerTask, UncommittedResultSet}, block_sequencer::VectorClock, cache_manager::{CacheCommand, CacheKey}, TxWithAckChanTag}};


pub struct SLIDETask {
    cache_connector: CacheConnector,
    id: usize,
    total_work: usize,
    total_workers_per_node: usize,
}

enum Response {
    Invalid(MsgAckChanWithTag),
    Receipt(MsgAckChanWithTag, Vec<ProtoTransactionOpResult>, Option<u64>),
}

impl ClientHandlerTask for SLIDETask {
    fn new(config: AtomicPSLWorkerConfig, cache_connector: CacheConnector, id: usize) -> Self {
        Self {
            cache_connector,
            id,
            total_work: 0,
            total_workers_per_node: config.get().worker_config.num_worker_threads_per_worker,
        }
    }

    fn get_locked_keys(&self) -> Vec<CacheKey> {
        vec![]
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


            tx_phases_resp.push((on_receive.clone(), resp.clone()));
        }

        let start_time = Instant::now();

        for (on_receive, resp) in tx_phases_resp {
            if let std::result::Result::Ok((results, seq_num, _atleast_one_write)) = self.execute_ops(on_receive.ops.as_ref()).await {
                response_vec.push(Response::Receipt(resp, results, seq_num));
            } else {
                response_vec.push(Response::Invalid(resp));
            }
    
        }

        trace!("Execute ops time: {:?}", start_time.elapsed());

        // Group commit. Supposed to improve throughput.
        let _vc = self.cache_connector.dispatch_commit_request(false).await;

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

impl SLIDETask {
    async fn execute_ops(&mut self, ops: &Vec<ProtoTransactionOp>) -> Result<(Vec<ProtoTransactionOpResult>, Option<u64>, bool), anyhow::Error> {
        let mut results = Vec::new();

        for op in ops {
            let op_type = op.op_type();

            match op_type {
                ProtoTransactionOpType::Custom => {
                    let training_data = &op.operands;
                    let my_name = self.cache_connector.blocking_client.0.config.get().net_config.name.clone();
                    let all_worker_list = self.cache_connector.blocking_client.0.config.get()
                        .net_config.nodes.keys()
                        .filter(|name| name.starts_with("node"))
                        .cloned()
                        .collect::<Vec<_>>();

                    let my_pos = all_worker_list.iter().position(|name| name == &my_name).unwrap();
                    let my_id = self.id;

                    let aggregate_id = my_pos * all_worker_list.len() + my_id;

                    // Split the training data into total_workers_per_node * all_worker_list.len() parts.
                    let num_parts = self.total_workers_per_node * all_worker_list.len();
                    let part_size = training_data.len() / num_parts;
                    let part_start = aggregate_id * part_size;
                    let mut part_end = part_start + part_size;
                    if aggregate_id == num_parts - 1 {
                        part_end = training_data.len();
                    }

                    if part_start >= training_data.len() {
                        return Err(anyhow::anyhow!("Part start is greater than training data length"));
                    }

                    if part_end > training_data.len() {
                        return Err(anyhow::anyhow!("Part end is greater than training data length"));
                    }

                    let training_part = training_data[part_start..part_end].to_vec();

                    // Nurzhan: Do training here.

                    results.push(ProtoTransactionOpResult { success: true, values: vec![] });
                }
                _ => {}
            }
            
        }


        Ok((results, None, false))
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