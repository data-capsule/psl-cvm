use std::{collections::{HashMap, HashSet}, pin::Pin};

use num_bigint::{BigInt, Sign};

use crate::crypto::hash;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DWWValue {
    pub(crate) value: Vec<u8>,
    pub(crate) seq_num: u64,
    pub(crate) val_hash: BigInt,
}

impl DWWValue {

    pub fn get_value(&self) -> Vec<u8> {
        self.value.clone()
    }

    /// Size estimate in bytes.
    pub fn size(&self) -> usize {
        self.value.len() + std::mem::size_of::<u64>() + self.val_hash.to_bytes_be().1.len()
    }

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
    pub fn blind_update(&mut self, new_value: Vec<u8>, new_val_hash: BigInt) -> u64 {
        self.value = new_value;
        self.seq_num += 1;
        self.val_hash = new_val_hash;

        self.seq_num
    }


    /// Merge with new value with new seq_num.
    /// Merge logic is: Higher seq_num or same seq_num with higher hash.
    /// Hash is calculated only when necessary. So this is not constant time.
    /// Returns Ok(new_seq_num) | Err(old_seq_num)
    pub fn merge(&mut self, new_value: Vec<u8>, new_seq_num: u64) -> Result<u64, u64> {
        if new_seq_num > self.seq_num {
            self.value = new_value;
            self.seq_num = new_seq_num;
            self.val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&self.value));
            return Ok(new_seq_num);
        } else if new_seq_num == self.seq_num {
            let new_hash = hash(&new_value);
            let new_hash_num = BigInt::from_bytes_be(Sign::Plus, &new_hash);
            if new_hash_num > self.val_hash {
                self.value = new_value;
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
        new_value: Self,
    ) -> Result<u64, u64> {
        if new_value.seq_num > self.seq_num {
            self.value = new_value.value;
            self.seq_num = new_value.seq_num;
            self.val_hash = new_value.val_hash;
            return Ok(self.seq_num);
        } else if new_value.seq_num == self.seq_num {
            if new_value.val_hash > self.val_hash {
                self.value = new_value.value;
                self.val_hash = new_value.val_hash;
                self.seq_num = new_value.seq_num;
                return Ok(self.seq_num);
            }
        }

        Err(self.seq_num)
    }

    pub fn merge_immutable(&self, new_value: &Self) -> Self {
        let mut val = self.clone();
        let _ = val.merge_cached(new_value.clone());
        val
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]

pub struct PNCounterValue {
    pub(crate) increment_value: HashMap<String /* origin */, f64>,
    pub(crate) decrement_value: HashMap<String /* origin */, f64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]

pub struct WrongPNCounterValue {
    pub(crate) value: f64,
}

impl PNCounterValue {
    pub fn new() -> Self {
        Self {
            increment_value: HashMap::new(),
            decrement_value: HashMap::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.increment_value.iter().map(|(key, _)| key.len() + std::mem::size_of::<f64>()).sum::<usize>()
        + self.decrement_value.iter().map(|(key, _)| key.len() + std::mem::size_of::<f64>()).sum::<usize>()
    }

    pub fn get_value(&self) -> f64 {
        let incr_value_total = self.increment_value.values().sum::<f64>();
        let decr_value_total = self.decrement_value.values().sum::<f64>();
        incr_value_total - decr_value_total
    }

    pub fn merge(&mut self, new_value: Self) -> Result<f64, f64> {
        let mut any_change = false;
        let all_incr_keys = self.increment_value.keys().chain(new_value.increment_value.keys()).cloned().collect::<HashSet<_>>();
        for key in &all_incr_keys {
            let val1 = *self.increment_value.get(key).unwrap_or(&0.0);
            let val2 = *new_value.increment_value.get(key).unwrap_or(&0.0);

            let final_val = val1.max(val2);
            if (final_val - val1) > 1e-6 {
                any_change = true;
            }
            self.increment_value.insert(key.clone(), final_val);
        }

        let all_decr_keys = self.decrement_value.keys().chain(new_value.decrement_value.keys()).cloned().collect::<HashSet<_>>();
        for key in &all_decr_keys {
            let val1 = *self.decrement_value.get(key).unwrap_or(&0.0);
            let val2 = *new_value.decrement_value.get(key).unwrap_or(&0.0);
            let final_val = val1.max(val2);
            if (final_val - val1) > 1e-6 {
                any_change = true;
            }
            self.decrement_value.insert(key.clone(), final_val);
        }

        // warn!("Merged PNCounterValue: {:?}", self);

        if any_change {
            Ok(self.get_value())
        } else {
            Err(self.get_value())
        }


    }

    pub fn merge_immutable(&self, new_value: &Self) -> Self {
        let mut val = self.clone();
        let _ = val.merge(new_value.clone());
        val
    }

    pub fn blind_increment(&mut self, origin: String, value: f64) -> f64 {
        assert!(value >= 0.0);
        let entry = self.increment_value.entry(origin).or_insert(0.0);
        *entry += value;
        *entry
    }

    pub fn blind_decrement(&mut self, origin: String, value: f64) -> f64 {
        assert!(value >= 0.0);
        let entry = self.decrement_value.entry(origin).or_insert(0.0);
        *entry += value;
        *entry
    }  
}

impl PartialEq for PNCounterValue {
    fn eq(&self, other: &Self) -> bool {
        self.get_value() == other.get_value()
    }
}

impl Eq for PNCounterValue {}


impl WrongPNCounterValue {
    pub fn new() -> Self {
        Self {
            value: 0.0,
        }
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<f64>()
    }

    pub fn get_value(&self) -> f64 {
        self.value
    }

    pub fn merge(&mut self, new_value: Self) -> Result<f64, f64> {
        let mut any_change = false;
        let final_val = self.value.max(new_value.value);
        if (final_val - self.value) > 1e-6 {
            any_change = true;
        }
        self.value = final_val;
        // warn!("Merged PNCounterValue: {:?}", self);
        if any_change {
            Ok(self.get_value())
        } else {
            Err(self.get_value())
        }

    }

    pub fn merge_immutable(&self, new_value: &Self) -> Self {
        let mut val = self.clone();
        let _ = val.merge(new_value.clone());
        val
    }

    pub fn blind_increment(&mut self, _origin: String, value: f64) {
        assert!(value >= 0.0);
        self.value += value;
    }

    pub fn blind_decrement(&mut self, _origin: String, value: f64) {
        assert!(value >= 0.0);
        self.value -= value;
    }
}

impl PartialEq for WrongPNCounterValue {
    fn eq(&self, other: &Self) -> bool {
        self.get_value() == other.get_value()
    }
}

impl Eq for WrongPNCounterValue {}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum CachedValue {
    DWW(DWWValue),
    PNCounter(PNCounterValue),
}

impl CachedValue {
    pub fn is_dww(&self) -> bool {
        matches!(self, CachedValue::DWW(_))
    }

    pub fn is_pn_counter(&self) -> bool {
        matches!(self, CachedValue::PNCounter(_))
    }

    pub fn get_dww(&self) -> Option<&DWWValue> {
        if let CachedValue::DWW(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_pn_counter(&self) -> Option<&PNCounterValue> {
        if let CachedValue::PNCounter(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_dww_mut(&mut self) -> Option<&mut DWWValue> {
        if let CachedValue::DWW(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_pn_counter_mut(&mut self) -> Option<&mut PNCounterValue> {
        if let CachedValue::PNCounter(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn new_dww(value: Vec<u8>, val_hash: BigInt) -> Self {
        Self::DWW(DWWValue::new(value, val_hash))
    }

    pub fn new_dww_with_seq_num(value: Vec<u8>, seq_num: u64, val_hash: BigInt) -> Self {
        Self::DWW(DWWValue::new_with_seq_num(value, seq_num, val_hash))
    }

    pub fn new_pn_counter() -> Self {
        Self::PNCounter(PNCounterValue::new())
    }

    pub fn from_pn_counter(value: PNCounterValue) -> Self {
        Self::PNCounter(value)
    }

    pub fn size(&self) -> usize {
        match self {
            CachedValue::DWW(value) => value.size(),
            CachedValue::PNCounter(value) => value.size(),
        }
    }
}



pub type CacheKey = Vec<u8>;
