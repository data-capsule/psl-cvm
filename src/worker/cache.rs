use std::sync::Mutex;

use hashbrown::HashMap;
use num_bigint::{BigInt, Sign};

use crate::crypto::hash;

pub type CacheKey = Vec<u8>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CachedValue {
    pub value: Vec<u8>,
    pub seq_num: u64,
    pub val_hash: BigInt,
}

impl CachedValue {

    /// Completely new value, with seq_num = 1
    pub fn new(value: Vec<u8>, val_hash: BigInt) -> Self {
        Self::new_with_seq_num(value, 1, val_hash)
    }

    pub fn new_with_seq_num(value: Vec<u8>, seq_num: u64, val_hash: BigInt) -> Self {
        Self {
            value,
            seq_num,
            val_hash,
        }
    }

    /// Blindly update value, incrementing seq_num
    pub fn blind_update(&mut self, new_value: &Vec<u8>, new_val_hash: &BigInt) -> u64 {
        self.value = new_value.clone();
        self.seq_num += 1;
        self.val_hash = new_val_hash.clone();

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
            self.val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&self.value));
            return Ok(new_seq_num);
        } else if new_seq_num == self.seq_num {
            let new_hash = hash(&new_value);
            let new_hash_num = BigInt::from_bytes_be(Sign::Plus, &new_hash);
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
        new_value: &CachedValue,
    ) -> Result<u64, u64> {
        if new_value.seq_num > self.seq_num {
            self.value.copy_from_slice(&new_value.value);
            self.seq_num = new_value.seq_num;
            self.val_hash = new_value.val_hash.clone();
            return Ok(self.seq_num);
        } else if new_value.seq_num == self.seq_num {
            if new_value.val_hash > self.val_hash {
                self.value.copy_from_slice(&new_value.value);
                self.val_hash = new_value.val_hash.clone();
                self.seq_num = new_value.seq_num;
                return Ok(self.seq_num);
            }
        }

        Err(self.seq_num)
    }
}

pub struct Cache {
    cache: Mutex<HashMap<CacheKey, CachedValue>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<CachedValue> {
        self.cache.lock().unwrap().get(key).cloned()
    }

    pub fn put(&self, key: CacheKey, value: CachedValue) {
        self.cache.lock().unwrap()
            .entry(key)
            .and_modify(|v| {
                v.merge_cached(&value).unwrap();
            })
            .or_insert(value);
    }

    pub fn put_raw(&self, key: CacheKey, value: Vec<u8>) {
        let mut cache = self.cache.lock().unwrap();

        let val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&value));
        cache.entry(key)
            .and_modify(|v| {
                v.blind_update(&value, &val_hash);
            })
            .or_insert(CachedValue::new(value, val_hash));
    }

    pub fn bulk_put(&self, key_value_pairs: Vec<(CacheKey, CachedValue)>) {
        let mut cache = self.cache.lock().unwrap();
        for (key, value) in key_value_pairs {
            cache.entry(key)
                .and_modify(|v| {
                    v.merge_cached(&value).unwrap();
                })
                .or_insert(value);
        }
    }


}