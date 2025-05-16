use std::sync::Arc;

use hashbrown::HashMap;
use num_bigint::{BigInt, Sign};
use thiserror::Error;
use tokio::sync::{oneshot, Mutex};
use crate::{crypto::{hash, CachedBlock, HashType}, proto::execution::{ProtoTransaction, ProtoTransactionOpType}, rpc::SenderType, utils::channel::{Receiver, Sender}};

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
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    ),
    Cas(
        CacheKey /* Key */,
        Vec<u8> /* Value */,
        u64 /* Expected SeqNum */,
        oneshot::Sender<Result<u64 /* seq_num */, CacheError>>,
    )
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
    ) -> anyhow::Result<()> {
        dispatch!(self, CacheCommand::Put, key, value);
        Ok(())
    }


}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct CachedValue {
    value: Vec<u8>,
    seq_num: u64,
    val_hash: BigInt,
}

impl CachedValue {

    /// Completely new value, with seq_num = 1
    pub fn new(value: Vec<u8>) -> Self {
        let val_hash = hash(&value);
        let val_hash = BigInt::from_bytes_be(Sign::NoSign, &val_hash);
        Self {
            value,
            seq_num: 1,
            val_hash,
        }
    }

    /// Blindly update value, incrementing seq_num
    pub fn blind_update(&mut self, new_value: Vec<u8>) -> u64 {
        self.value = new_value;
        self.seq_num += 1;
        self.val_hash = BigInt::from_bytes_be(Sign::NoSign, &hash(&self.value));

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
            self.val_hash = BigInt::from_bytes_be(Sign::NoSign, &hash(&self.value));
            return Ok(new_seq_num);
        } else if new_seq_num == self.seq_num {
            let new_hash = hash(&new_value);
            let new_hash_num = BigInt::from_bytes_be(Sign::NoSign, &new_hash);
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
}

pub type CacheKey = Vec<u8>;

pub struct CacheManager {
    command_rx: Receiver<CacheCommand>,
    block_rx: Receiver<(SenderType, CachedBlock)>,
    cache: HashMap<CacheKey, CachedValue>,
}

impl CacheManager {
    pub fn new(
        command_rx: Receiver<CacheCommand>,
        block_rx: Receiver<(SenderType, CachedBlock)>,
    ) -> Self {
        Self {
            command_rx,
            block_rx,
            cache: HashMap::new(),
        }
    }

    pub async fn run(cache_manager: Arc<Mutex<CacheManager>>) {
        let mut cache_manager = cache_manager.lock().await;
        
        
        while let Ok(_) = cache_manager.worker().await {
            // Handle errors if needed
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            Some(command) = self.command_rx.recv() => {
                self.handle_command(command).await;
            }
            Some((sender, block)) = self.block_rx.recv() => {
                self.handle_block(sender, block).await;
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: CacheCommand) {
        match command {
            CacheCommand::Get(key, response_tx) => {
                let res = self.cache.get(&key).map(|v| (v.value.clone(), v.seq_num));
                let _ = response_tx.send(res.ok_or(CacheError::KeyNotFound));
            }
            CacheCommand::Put(key, value, response_tx) => {
                if self.cache.contains_key(&key) {
                    let seq_num = self.cache.get_mut(&key).unwrap().blind_update(value);
                    response_tx.send(Ok(seq_num)).unwrap();

                    return;
                }

                let cached_value = CachedValue::new(value);
                self.cache.insert(key.clone(), cached_value);
                response_tx.send(Ok(1)).unwrap();
            }
            CacheCommand::Cas(key, value, expected_seq_num, response_tx) => {
                unimplemented!();
            }
        }
    }

    async fn handle_block(&mut self, sender: SenderType, block: CachedBlock) {
        for tx in &block.block.tx_list {
            if tx.on_crash_commit.is_none() {
                continue;
            }

            let ops = tx.on_crash_commit.as_ref().unwrap();
            for op in &ops.ops {
                if op.op_type != ProtoTransactionOpType::Write as i32 {
                    continue;
                }

                if op.operands.len() != 2 {
                    continue;
                }

                let key = &op.operands[0];
                let value = &op.operands[1];

                let cached_value = bincode::deserialize::<CachedValue>(&value);
                if cached_value.is_err() {
                    continue;
                }

                let cached_value = cached_value.unwrap();

                if self.cache.contains_key(key) {
                    self.cache.get_mut(key).unwrap().merge_cached(cached_value).unwrap();
                } else {
                    self.cache.insert(key.clone(), cached_value);
                }
            }
        }
    }
}

