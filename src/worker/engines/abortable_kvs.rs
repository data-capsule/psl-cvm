use std::{collections::HashSet, time::Instant};

use futures::{stream::FuturesUnordered, StreamExt as _};
use itertools::Itertools;
use log::{error, info, trace, warn};
use rand::Rng;
use rustls::crypto::CryptoProvider;
use tokio::sync::oneshot;

use crate::{consensus::batch_proposal::MsgAckChanWithTag, proto::execution::{ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType}, utils::channel::Sender, worker::{app::{CacheConnector, ClientHandlerTask, UncommittedResultSet}, block_sequencer::VectorClock, cache_manager::CacheCommand, TxWithAckChanTag}};
use crate::utils::types::CacheKey;


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

        // let vc = if all_reads {
        //     VectorClock::new() // Doesn't matter. If all reads, there are no locked keys.
        // } else {
        //     vc
        // };
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
                ProtoTransactionOpType::Read => {
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

                #[cfg(feature = "app_key_transparency")]
                ProtoTransactionOpType::StoredProcedure1 => {
                    atleast_one_write = true;
                    last_write_index = i;
                },

                #[cfg(feature = "app_banking")]
                ProtoTransactionOpType::StoredProcedure1 => {
                    atleast_one_write = true;
                    last_write_index = i;
                },
                #[cfg(feature = "app_banking")]
                ProtoTransactionOpType::StoredProcedure2 => {
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
                    let (res, block_seq_num_rx) = self.cache_connector.dispatch_read_request(key).await;
                    match res {
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
                    if let Some(block_seq_num_rx) = block_seq_num_rx {
                        block_seq_num_rx_vec.push(block_seq_num_rx);
                    }
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

                    self.checked_decrement(key, value, &mut is_aborted, &mut results, &mut block_seq_num_rx_vec, &mut locked_keys).await;
                    

                    // The caller takes care of unlocking.
                },

                #[cfg(feature = "app_banking")]
                ProtoTransactionOpType::StoredProcedure1 => {
                    // This is WriteCheck(custid, amount) in Smallbank.
                    let custid = op.operands[0].clone();
                    let amount = op.operands[1].clone();
                    let custid = u64::from_be_bytes(custid.as_slice().try_into().unwrap());
                    let amount = f64::from_be_bytes(amount.as_slice().try_into().unwrap());
                    self.smallbank_write_check(custid, amount, &mut is_aborted, &mut results, &mut block_seq_num_rx_vec, &mut locked_keys).await;

                }

                #[cfg(feature = "app_banking")]
                ProtoTransactionOpType::StoredProcedure2 => {
                    // This is Amalgamate(custid1, custid2) in Smallbank.
                    let custid1 = op.operands[0].clone();
                    let custid2 = op.operands[1].clone();
                    let custid1 = u64::from_be_bytes(custid1.as_slice().try_into().unwrap());
                    let custid2 = u64::from_be_bytes(custid2.as_slice().try_into().unwrap());
                    let success = self.smallbank_amalgamate(custid1, custid2, &mut is_aborted, &mut results, &mut block_seq_num_rx_vec, &mut locked_keys).await;

                    if !success {
                        is_aborted = true;
                        trace!("Amalgamate failed: {} and {}", custid1, custid2);
                    }

                }

                #[cfg(feature = "app_key_transparency")]
                ProtoTransactionOpType::StoredProcedure1 => {
                    let user = op.operands[0].clone();
                    self.key_transparency_refresh_key(user, &mut is_aborted, &mut results, &mut block_seq_num_rx_vec, &mut locked_keys).await;
                }

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
                if seq_num == u64::MAX {
                    continue;
                }
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


    async fn checked_decrement(&mut self, key: CacheKey, value: f64, is_aborted: &mut bool, results: &mut Vec<ProtoTransactionOpResult>, block_seq_num_rx_vec: &mut FuturesUnordered<oneshot::Receiver<u64>>, locked_keys: &mut Vec<CacheKey>) -> bool /* success */{
        let (approx_curr_val, block_seq_num_rx) = self.cache_connector.dispatch_counter_read_request(key.clone(), false).await;
        
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }

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
                return false;
            }
        };

        if approx_curr_val < value {
            // Abort everything after this.
            trace!("Counter: {} Approximate unlocked value: {} is less than decrement value: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), approx_curr_val, value);
            *is_aborted = true;
            results.push(ProtoTransactionOpResult {
                success: false,
                values: vec![],
            });
            return false;
        }

        let _ = self.cache_connector.dispatch_lock_request(&vec![(key.clone(), false)]).await;
        locked_keys.push(key.clone());

        let (curr_val, block_seq_num_rx) = self.cache_connector.dispatch_counter_read_request(key.clone(), true).await;
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }
        
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
            trace!("Counter: {} Current locked value: {} is less than decrement value: {}", String::from_utf8(key.clone()).unwrap_or(hex::encode(key)), curr_val, value);
            *is_aborted = true;
            results.push(ProtoTransactionOpResult {
                success: false,
                values: vec![],
            });
            return false;
        }

        let res = self.cache_connector.dispatch_decrement_request(key.clone(), value).await;
        if let std::result::Result::Err(_e) = res {
            return false;
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
        true
    
    }

    fn get_checking_table_key(custid: u64) -> String {
        format!("CHECKING:{}", custid)
    }

    fn get_savings_table_key(custid: u64) -> String {
        format!("SAVINGS:{}", custid)
    }

    async fn smallbank_write_check(&mut self, custid: u64, amount: f64, is_aborted: &mut bool, results: &mut Vec<ProtoTransactionOpResult>, block_seq_num_rx_vec: &mut FuturesUnordered<oneshot::Receiver<u64>>, locked_keys: &mut Vec<CacheKey>) {
        let checking_key = Self::get_checking_table_key(custid);
        let savings_key = Self::get_savings_table_key(custid);

        let (checking_val, block_seq_num_rx) = self.cache_connector.dispatch_counter_read_request(checking_key.clone().into_bytes(), false).await;
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }
        let (savings_val, block_seq_num_rx) = self.cache_connector.dispatch_counter_read_request(savings_key.clone().into_bytes(), false).await;
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }

        let Ok(checking_val) = checking_val else {
            *is_aborted = true;
            return;
        };
        let Ok(savings_val) = savings_val else {
            *is_aborted = true;
            return;
        };

        let (checking_deduct, savings_deduct) = if checking_val + savings_val < amount {
            (checking_val, savings_val) // zero out both
        } else if checking_val < amount {
            (checking_val, amount - checking_val) // zero out checking, take the rest from savings
        } else {
            (amount, 0.0) // take the full amount from checking, don't touch savings
        };


        if checking_deduct > 0.0 {
            let ret = self.checked_decrement(checking_key.into_bytes(), checking_deduct, is_aborted, results, block_seq_num_rx_vec, locked_keys).await;
            if !ret {
                return;
            }
        }
        if savings_deduct > 0.0 {
            self.checked_decrement(savings_key.into_bytes(), savings_deduct, is_aborted, results, block_seq_num_rx_vec, locked_keys).await;
        }

        trace!("WriteCheck: {} deducted {} from checking and {} from savings", custid, checking_deduct, savings_deduct);
    }

    async fn smallbank_amalgamate(&mut self, custid1: u64, custid2: u64, is_aborted: &mut bool, results: &mut Vec<ProtoTransactionOpResult>, block_seq_num_rx_vec: &mut FuturesUnordered<oneshot::Receiver<u64>>, locked_keys: &mut Vec<CacheKey>) -> bool /* success */ {
        let src_checking_key = Self::get_checking_table_key(custid1);
        let src_savings_key = Self::get_savings_table_key(custid1);
        let dst_checking_key = Self::get_checking_table_key(custid2);
        let dst_savings_key = Self::get_savings_table_key(custid2);

        // Zero out src_* and dst_savings.
        let (src_checking_val, block_seq_num_rx) = self.cache_connector.dispatch_counter_read_request(src_checking_key.clone().into_bytes(), false).await;
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }
        let (src_savings_val, block_seq_num_rx) = self.cache_connector.dispatch_counter_read_request(src_savings_key.clone().into_bytes(), false).await;
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }
        let (dst_savings_val, block_seq_num_rx) = self.cache_connector.dispatch_counter_read_request(dst_savings_key.clone().into_bytes(), false).await;
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }

        let Ok(src_checking_val) = src_checking_val else {
            *is_aborted = true;
            return false;
        };
        let Ok(src_savings_val) = src_savings_val else {
            *is_aborted = true;
            return false;
        };
        let Ok(dst_savings_val) = dst_savings_val else {
            *is_aborted = true;
            return false;
        };


        if src_checking_val > 0.0 {
            let ret = self.checked_decrement(src_checking_key.clone().into_bytes(), src_checking_val, is_aborted, results, block_seq_num_rx_vec, locked_keys).await;
            if !ret {
                return false;
            }
        }
        if src_savings_val > 0.0 {
            let ret = self.checked_decrement(src_savings_key.clone().into_bytes(), src_savings_val, is_aborted, results, block_seq_num_rx_vec, locked_keys).await;
            if !ret {

                // Rollback src_checking_val.
                if src_checking_val > 0.0 {
                    let _ = self.cache_connector.dispatch_blind_increment_request(src_checking_key.clone().into_bytes(), src_checking_val).await;
                }
                return false;
            }
        }
        if dst_savings_val > 0.0 {
            let ret = self.checked_decrement(dst_savings_key.clone().into_bytes(), dst_savings_val, is_aborted, results, block_seq_num_rx_vec, locked_keys).await;
            if !ret {
                // Rollback src_checking_val.
                if src_checking_val > 0.0 {
                    let _ = self.cache_connector.dispatch_blind_increment_request(src_checking_key.clone().into_bytes(), src_checking_val).await;
                }
                // Rollback src_savings_val.
                if src_savings_val > 0.0 {
                    let _ = self.cache_connector.dispatch_blind_increment_request(src_savings_key.clone().into_bytes(), src_savings_val).await;
                }
                return false;
            }
        }

        let total_deducted_val = src_checking_val + src_savings_val + dst_savings_val;

        if total_deducted_val > 0.0 {
            let res = self.cache_connector.dispatch_increment_request(dst_checking_key.clone().into_bytes(), total_deducted_val).await;
            if let std::result::Result::Err(e) = res {
                // This generally never fails.
                return false;
            }
            let (approx_curr_val, block_seq_num_rx) = res.unwrap();
            if let Some(block_seq_num_rx) = block_seq_num_rx {
                block_seq_num_rx_vec.push(block_seq_num_rx);
            }
    
            results.push(ProtoTransactionOpResult {
                success: true,
                values: vec![approx_curr_val.to_be_bytes().to_vec()],
            });
        }


        trace!("Amalgamate: Transferred {} from {} to {}", total_deducted_val, custid1, custid2);
        true


    }

    // Generate a random key for the user.
    // Store the public key.
    // Return the private key.
    // Then fire a ACK_BARRIER.
    // Wait till all nodes see the new write.
    async fn key_transparency_refresh_key(&mut self, user: Vec<u8>, is_aborted: &mut bool, results: &mut Vec<ProtoTransactionOpResult>, block_seq_num_rx_vec: &mut FuturesUnordered<oneshot::Receiver<u64>>, _locked_keys: &mut Vec<CacheKey>) {
        use rand::rng;
        use ed25519_dalek::SigningKey;

        let secret_key: [u8; 32] = rng().random();
        let signing_key = SigningKey::from_bytes(&secret_key);
        let public_key = signing_key.verifying_key();


        // Next part is similar to a DWW write.
        let key = user.clone();
        let value = public_key.to_bytes().to_vec();
        let res = self.cache_connector.dispatch_write_request(key, value).await;
        if let std::result::Result::Err(_e) = res {
            *is_aborted = true;
            return;
        }

        let (_, block_seq_num_rx) = res.unwrap();
        if let Some(block_seq_num_rx) = block_seq_num_rx {
            block_seq_num_rx_vec.push(block_seq_num_rx);
        }
        results.push(ProtoTransactionOpResult {
            success: true,
            values: vec![],
        });

        // Now send an ACK_BARRIER with key = "barrier:{public_key}"
        // let barrier_key = format!("barrier:{}", hex::encode(public_key.to_bytes()));
        // let barrier_key = barrier_key.as_bytes().to_vec();
        // let total_workers = self.cache_connector.blocking_client.0 // Very roundabout way to get to the config.
        //     .config.get().net_config.nodes.keys()
        //     .filter(|name| name.starts_with("node"))
        //     .count();
        // let (res, waiter_rx) = self.cache_connector.dispatch_ack_barrier_request(barrier_key.clone(), total_workers as f64).await;
        // // block_seq_num_rx_vec.push(waiter_rx);
        // if let std::result::Result::Err(_e) = res {
        //     *is_aborted = true;
        //     return;
        // }
    }
}