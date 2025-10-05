use std::collections::HashMap;

use lru::LruCache;
use serde::Serialize;

use crate::{config::RocksDBConfig, utils::types::{CacheKey, CachedValue}};
use rocksdb::{DBCompactionStyle, Options, WriteBatchWithTransaction, WriteOptions, DB};


pub struct Cache {
    read_cache: LruCache<CacheKey, CachedValue>,
    db: DB,
}

impl Cache {
    pub fn new(cache_size: usize, config: RocksDBConfig) -> Self {
        let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.set_write_buffer_size(config.write_buffer_size);
            opts.set_max_write_buffer_number(config.max_write_buffer_number);
            opts.set_min_write_buffer_number_to_merge(config.max_write_buffers_to_merge);
            opts.set_target_file_size_base(config.write_buffer_size as u64);

            opts.set_manual_wal_flush(true);
            opts.set_compaction_style(DBCompactionStyle::Universal);
            opts.set_allow_mmap_reads(true);
            opts.set_allow_mmap_writes(true);

            // opts.increase_parallelism(3);

            let path = config.db_path.clone();
            let db = DB::open(&opts, path).unwrap();

        Self { read_cache: LruCache::new(std::num::NonZero::new(cache_size).unwrap()), db }
    }

    pub fn get(&mut self, key: &CacheKey) -> (Option<CachedValue>, bool /* read from cache */) {
        let val = self.read_cache.get(key);
        if val.is_some() {
            return (val.cloned(), true);
        }

        let val = self.db.get(key.clone()).unwrap();

        if val.is_none() {
            return (None, false);
        }

        let val: CachedValue = bincode::deserialize(&val.unwrap()).unwrap();
        self.read_cache.put(key.clone(), val.clone());
        (Some(val), false)
    }

    pub fn put(&mut self, key: CacheKey, value: CachedValue) {
        let mut wopts = WriteOptions::default();
        wopts.disable_wal(true);

        let ser = bincode::serialize(&value).unwrap();
        self.db.put_opt(key.clone(), ser, &wopts).unwrap();
        self.read_cache.put(key, value);
    }

    pub fn len(&self) -> usize {
        self.read_cache.len()
    }

    pub fn contains_key(&self, key: &CacheKey) -> bool {
        self.read_cache.contains(key)
        || self.db.key_may_exist(key)
    }

    pub fn stats(&self) -> Vec<String> {
        let (max_seq_num, max2_seq_num, max_key, max2_key) = self.read_cache.iter()
        .filter(|(_, val)| val.is_dww())
        .fold((0u64, 0u64, CacheKey::new(), CacheKey::new()), |acc, (key, val)| {
            let val = val.get_dww().unwrap();
            if val.seq_num > acc.0 {
                (val.seq_num, acc.0, key.clone(), acc.2)
            } else if val.seq_num > acc.1 {
                (acc.0, val.seq_num, acc.2, key.clone())
            } else {
                acc
            }
        });

        let rocksdb_stats = self.db.property_value("rocksdb.stats").unwrap_or_default().unwrap_or_default();
        let rocksdb_stat_lines = rocksdb_stats.split('\n').collect::<Vec<&str>>();
        let _n = rocksdb_stat_lines.len();
        let rocksdb_stats = rocksdb_stat_lines[_n-10..].join("\n");
        vec![
            format!("Read Cache size: {}, Max seq num: {} with Key: {}, Second max seq num: {} with Key: {}",
                self.read_cache.len(),
                max_seq_num, String::from_utf8(max_key.clone()).unwrap_or(hex::encode(max_key)),
                max2_seq_num, String::from_utf8(max2_key.clone()).unwrap_or(hex::encode(max2_key))
            ),

            format!("RocksDB stats: {}", rocksdb_stats),
        ]
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        let _ = self.db.flush();
        let opts = Options::default();

        let _ = DB::destroy(&opts, "./node_db");
    }
}