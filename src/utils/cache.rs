use std::collections::HashMap;

use lru::LruCache;
use serde::Serialize;
use tokio::sync::oneshot;

use crate::{config::RocksDBConfig, utils::{channel::make_channel, types::{CacheKey, CachedValue}}};
// use rocksdb::{DBCompactionStyle, Options, WriteBatchWithTransaction, WriteOptions, DB};
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle};


enum PersistentCommand {
    Put(CacheKey, CachedValue, oneshot::Sender<()>),
    Get(CacheKey, oneshot::Sender<Option<CachedValue>>),
    ContainsKey(CacheKey, oneshot::Sender<bool>),
    Stats(oneshot::Sender<Vec<String>>),
    Drop(oneshot::Sender<()>),
}

pub struct Cache {
    read_cache: LruCache<CacheKey, CachedValue>,
    persistent_tx: tokio::sync::mpsc::Sender<PersistentCommand>,
    // db_path: String,
    // db: PartitionHandle,
    // keyspace: Keyspace,
}

impl Cache {
    pub fn new(cache_size: usize, config: RocksDBConfig) -> Self {
        // let mut opts = Options::default();
        // opts.create_if_missing(true);
        // opts.set_write_buffer_size(config.write_buffer_size);
        // opts.set_max_write_buffer_number(config.max_write_buffer_number);
        // opts.set_min_write_buffer_number_to_merge(config.max_write_buffers_to_merge);
        // opts.set_target_file_size_base(config.write_buffer_size as u64);

        // opts.set_manual_wal_flush(true);
        // opts.set_compaction_style(DBCompactionStyle::Universal);
        // opts.set_allow_mmap_reads(true);
        // opts.set_allow_mmap_writes(true);

        // opts.increase_parallelism(3);

        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);

        std::thread::spawn(move || {
            let fjall_config = fjall::Config::new(config.db_path.clone())
                .cache_size(config.write_buffer_size as u64)
                .compaction_workers(1)
                .flush_workers(1);
    
            let keyspace = fjall_config.open().unwrap();
            let db = keyspace.open_partition("default", PartitionCreateOptions::default()).unwrap();
        
            while let Some(cmd) = rx.blocking_recv() {
                match cmd {
                    PersistentCommand::Put(key, value, resp_tx) => {
                        let ser = bincode::serialize(&value).unwrap();
                        db.insert(key.clone(), ser).unwrap();
                        resp_tx.send(()).unwrap();
                    }
                    PersistentCommand::Get(key, resp_tx) => {
                        let val = db.get(key.clone()).unwrap();
                        match val {
                            Some(val) => {
                                let val = bincode::deserialize(&val).unwrap();
                                resp_tx.send(val).unwrap();
                            }
                            None => {
                                resp_tx.send(None).unwrap();
                            }
                        }
                    }
                    PersistentCommand::ContainsKey(key, resp_tx) => {
                        let val = db.contains_key(key.clone()).unwrap();
                        resp_tx.send(val).unwrap();
                    }
                    PersistentCommand::Stats(resp_tx) => {
                        let stats = vec![];
                        resp_tx.send(stats).unwrap();
                    }
                    PersistentCommand::Drop(resp_tx) => {
                        resp_tx.send(());
                        return;
                    }
                }
            }
        
        });

        // let path = config.db_path.clone();

        // let db = DB::open(&opts, path).unwrap();

        Self { read_cache: LruCache::new(std::num::NonZero::new(cache_size).unwrap()), persistent_tx: tx }
    }

    async fn db_get(&mut self, key: &CacheKey) -> Option<CachedValue> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.persistent_tx.send(PersistentCommand::Get(key.clone(), tx)).await.unwrap();
        rx.await.unwrap()
    }

    async fn db_put(&mut self, key: CacheKey, value: CachedValue) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.persistent_tx.send(PersistentCommand::Put(key.clone(), value.clone(), tx)).await.unwrap();
        rx.await.unwrap()
    }

    async fn db_contains_key(&self, key: &CacheKey) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.persistent_tx.send(PersistentCommand::ContainsKey(key.clone(), tx)).await.unwrap();
        rx.await.unwrap()
    }

    pub async fn get(&mut self, key: &CacheKey) -> (Option<CachedValue>, bool /* read from cache */) {
        let val = self.read_cache.get(key);
        if val.is_some() {
            return (val.cloned(), true);
        }

        let val = self.db_get(key).await;

        if val.is_none() {
            return (None, false);
        }

        // let val: CachedValue = bincode::deserialize(&val.unwrap()).unwrap();
        self.read_cache.put(key.clone(), val.clone().unwrap());
        (val, false)
    }

    pub async fn put(&mut self, key: CacheKey, value: CachedValue) {
        // let mut wopts = WriteOptions::default();
        // wopts.disable_wal(false);

        // let ser = bincode::serialize(&value).unwrap();
        self.db_put(key.clone(), value.clone()).await;
        self.read_cache.put(key, value);
    }

    pub fn len(&self) -> usize {
        self.read_cache.len()
    }

    /// Has to be 100% accurate.
    pub async fn contains_key(&self, key: &CacheKey) -> bool {
        if self.read_cache.contains(key) {
            return true;
        }

        // if !self.db.contains_key(key).unwrap() {
        //     return false;
        // }

        self.db_contains_key(key).await

        // self.db.get_pinned(key).unwrap().is_some()
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

        // let rocksdb_stats = self.db.property_value("rocksdb.stats").unwrap_or_default().unwrap_or_default();
        // let rocksdb_stat_lines = rocksdb_stats.split('\n').collect::<Vec<&str>>();
        // let _n = rocksdb_stat_lines.len();
        // let rocksdb_stats = rocksdb_stat_lines[_n-10..].join("\n");
        vec![
            format!("Read Cache size: {}, Max seq num: {} with Key: {}, Second max seq num: {} with Key: {}",
                self.read_cache.len(),
                max_seq_num, String::from_utf8(max_key.clone()).unwrap_or(hex::encode(max_key)),
                max2_seq_num, String::from_utf8(max2_key.clone()).unwrap_or(hex::encode(max2_key))
            ),

            // format!("RocksDB stats: {}", rocksdb_stats),
        ]
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        // let _ = self.db.flush();
        // let opts = Options::default();

        // let _ = DB::destroy(&opts, &self.db_path);
    }
}

