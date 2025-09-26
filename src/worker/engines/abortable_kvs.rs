use std::{collections::HashSet, time::Instant};

use futures::{stream::FuturesUnordered, StreamExt as _};
use itertools::Itertools;
use log::{error, info, trace, warn};

use crate::{consensus::batch_proposal::MsgAckChanWithTag, proto::execution::{ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType}, utils::channel::Sender, worker::{app::{CacheConnector, ClientHandlerTask, UncommittedResultSet}, block_sequencer::VectorClock, cache_manager::{CacheCommand, CacheKey}, TxWithAckChanTag}};


/// This is almost identical to KVSTask, but with subtle differences:
/// - Once an op in a transaction fails, all subsequent ops are automatically aborted.
/// - It supports INCREMENT AND CHECKED_DECREMENT operations on PN counters.
/// - CHECKED_DECREMENT uses locking to prevent the value from going below 0.
/// It may prematurely abort.
/// 
/// The checked decrement algorithm is as follows:
/// 1. approx_curr_val <- READ(counter) // unlocked
/// 2. if approx_curr_val < decr_val, abort
/// 3. LOCK(counter)
/// 4. curr_val <- READ(counter)
/// 5. if curr_val >= decr_val, DECREMENT(counter, decr_val)
/// 6. UNLOCK(counter)
pub struct AbortableKVSTask {
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

impl ClientHandlerTask for AbortableKVSTask {
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

            tx_phases_resp.push((on_receive.clone(), resp.clone()));
        }


        let start_time = Instant::now();

        let mut all_reads = true;

        let mut locked_keys = Vec::new();

        for (on_receive, resp) in tx_phases_resp {
    
            if let std::result::Result::Ok((results, seq_num, atleast_one_write, _locked_keys)) = self.execute_ops(on_receive.ops.as_ref()).await {
                locked_keys.extend(_locked_keys);
                
                if atleast_one_write {
                    all_reads = false;
                }
                response_vec.push(Response::Receipt(resp, results, seq_num));
            } else {
                response_vec.push(Response::Invalid(resp));
            }
    
        }

        trace!("Execute ops time: {:?}", start_time.elapsed());


        // Some checks.
        if all_reads {
            assert!(locked_keys.len() == 0);
        }

        // if locked_keys.len() > 1, there is a chance of deadlock. And we don't do deadlock detection.
        //
        // This is not strict and conservative 2PL anymore.

        // Group commit. Supposed to improve throughput.
        locked_keys.reverse();
        let start_time = Instant::now();
        let vc = self.cache_connector.dispatch_commit_request(locked_keys.len() > 0).await;
        trace!("Committed with VC: {} Locked keys: {:?}", vc,
            locked_keys.iter().map(|key| String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone()))).collect::<Vec<_>>());

        let vc = if all_reads {
            VectorClock::new() // Doesn't matter. If all reads, there are no locked keys.
        } else {
            vc
        };
        self.cache_connector.dispatch_unlock_request(locked_keys.iter().map(|key| (key.clone(), vc.clone())).collect()).await;
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

impl AbortableKVSTask {
    async fn execute_ops(&mut self, ops: &Vec<ProtoTransactionOp>) -> Result<(Vec<ProtoTransactionOpResult>, Option<u64>, bool, Vec<CacheKey> /* locked keys */), anyhow::Error> {
        let mut atleast_one_write = false;
        let mut last_write_index = 0;
        let mut highest_committed_block_seq_num_needed = 0;
        let mut block_seq_num_rx_vec = FuturesUnordered::new();
        let mut results = Vec::new();
        let mut locked_keys = Vec::new();
        let mut is_aborted = false;

        for (i, op) in ops.iter().enumerate() {
            let op_type = op.op_type();

            match op_type {
                ProtoTransactionOpType::Write  => {
                    atleast_one_write = true;
                    last_write_index = i;
                },
                ProtoTransactionOpType::Increment => {
                    atleast_one_write = true;
                    last_write_index = i;
                },
                ProtoTransactionOpType::CheckedDecrement => {
                    atleast_one_write = true;
                    last_write_index = i;
                },
                _ => {}
            }
        }

        for op in ops {
            if is_aborted {
                // Preserve invariant: #ops == #results.
                results.push(ProtoTransactionOpResult {
                    success: false,
                    values: vec![],
                });
                continue;
            }
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
                ProtoTransactionOpType::Increment => {
                    let key = op.operands[0].clone();
                    let value = op.operands[1].clone();
                    let value = f64::from_be_bytes(value.as_slice().try_into().unwrap());
                    if value < 0.0 {
                        warn!("Increment value is negative: {}", value);
                        results.push(ProtoTransactionOpResult {
                            success: false,
                            values: vec![],
                        });
                        continue;
                    }
                    let res = self.cache_connector.dispatch_increment_request(key.clone(), value).await;
                    if let std::result::Result::Err(e) = res {
                        return Err(e.into());
                    }
                    let (approx_curr_val, block_seq_num_rx) = res.unwrap();
                    if let Some(block_seq_num_rx) = block_seq_num_rx {
                        block_seq_num_rx_vec.push(block_seq_num_rx);
                    }

                    trace!("Counter: {} incremented to {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), approx_curr_val);
                    results.push(ProtoTransactionOpResult {
                        success: true,
                        values: vec![approx_curr_val.to_be_bytes().to_vec()],
                    });
                },
                ProtoTransactionOpType::CheckedDecrement => {
                    let key = op.operands[0].clone();
                    let value = op.operands[1].clone();
                    let value = f64::from_be_bytes(value.as_slice().try_into().unwrap());
                    if value < 0.0 {
                        warn!("Decrement value is negative: {}", value);
                        results.push(ProtoTransactionOpResult {
                            success: false,
                            values: vec![],
                        });
                        continue;
                    }
                    let approx_curr_val = self.cache_connector.dispatch_counter_read_request(key.clone(), false).await;
                    let approx_curr_val = match approx_curr_val {
                        std::result::Result::Ok(v) => v,
                        std::result::Result::Err(crate::worker::cache_manager::CacheError::KeyNotFound) => {
                            0.0
                        },
                        _ => {
                            warn!("Error reading counter: {:?}", approx_curr_val);
                            results.push(ProtoTransactionOpResult {
                                success: false,
                                values: vec![],
                            });
                            continue;
                        }
                    };

                    if approx_curr_val < value {
                        // Abort everything after this.
                        warn!("Counter: {} Approximate unlocked value: {} is less than decrement value: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), approx_curr_val, value);
                        is_aborted = true;
                        results.push(ProtoTransactionOpResult {
                            success: false,
                            values: vec![],
                        });
                        continue;
                    }

                    let _ = self.cache_connector.dispatch_lock_request(&vec![(key.clone(), false)]).await;
                    locked_keys.push(key.clone());

                    let curr_val = self.cache_connector.dispatch_counter_read_request(key.clone(), true).await;
                    let curr_val = match curr_val {
                        std::result::Result::Ok(v) => v,
                        std::result::Result::Err(crate::worker::cache_manager::CacheError::KeyNotFound) => {
                            0.0
                        },
                        _ => {
                            unreachable!()
                        }
                    };

                    if curr_val < value {
                        warn!("Counter: {} Current locked value: {} is less than decrement value: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), curr_val, value);
                        is_aborted = true;
                        results.push(ProtoTransactionOpResult {
                            success: false,
                            values: vec![],
                        });
                        continue;
                    }

                    let res = self.cache_connector.dispatch_decrement_request(key.clone(), value).await;
                    if let std::result::Result::Err(e) = res {
                        return Err(e.into());
                    }
                    let (final_val, block_seq_num_rx) = res.unwrap();
                    if let Some(block_seq_num_rx) = block_seq_num_rx {
                        block_seq_num_rx_vec.push(block_seq_num_rx);
                    }
                    trace!("Counter: {} decremented to {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), final_val);

                    results.push(ProtoTransactionOpResult {
                        success: true,
                        values: vec![final_val.to_be_bytes().to_vec()],
                    });

                    // The caller takes care of unlocking.
                },
                _ => {}
            }
            
        }

        if atleast_one_write {

            // Find the highest block seq num needed.
            while let Some(seq_num) = block_seq_num_rx_vec.next().await {
                if seq_num.is_err() {
                    continue;
                }

                let seq_num = seq_num.unwrap();
                highest_committed_block_seq_num_needed = std::cmp::max(highest_committed_block_seq_num_needed, seq_num);
            }

            return Ok((results, Some(highest_committed_block_seq_num_needed), atleast_one_write, locked_keys));
        }

        Ok((results, None, atleast_one_write, locked_keys))
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