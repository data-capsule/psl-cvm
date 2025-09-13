use std::{collections::{HashMap, HashSet}, ops::Deref, sync::{atomic::AtomicUsize, Arc}};
use num_bigint::{BigInt, Sign};
use tokio::sync::RwLock;
use dashmap::{DashMap, DashSet};
use log::{trace, warn};
use crate::{crypto::default_hash, worker::{block_sequencer::VectorClock, cache_manager::{CacheKey, CachedValue}}};


pub(super) struct ValueLattice {
    values: HashMap<VectorClock, CachedValue>,
    finalized_value: Option<CachedValue>,
    finalized_vc: VectorClock,
}

impl ValueLattice {
    fn new() -> Self {
        Self { values: HashMap::new(), finalized_value: None, finalized_vc: VectorClock::new() }
    }
}

pub struct _SnapshotStore {
    /// worker name => snapshot store.
    pub(super) store: DashMap<String, RwLock<HashMap<CacheKey, ValueLattice>>>,

    /// All VCs currently installed, reverse indexed by worker name.
    log: DashMap<VectorClock, String>,
    /// All VCs that have been garbage collected.
    __ghost_log: DashSet<VectorClock>,
    __ghost_reborn_counter: AtomicUsize,

    pub(super) global_lock: tokio::sync::Mutex<()>,
}


/// This will be shared across all auditors.
#[derive(Clone)]
pub struct SnapshotStore(Arc<_SnapshotStore>);

impl SnapshotStore {
    pub fn new(worker_names: Vec<String>) -> Self {
        let log = DashMap::new();
        let store = worker_names.iter().map(|name| (name.clone(), RwLock::new(HashMap::new()))).collect();

        log.insert(VectorClock::new(), worker_names[0].clone());
        Self(Arc::new(_SnapshotStore { store, log, __ghost_log: DashSet::new(), __ghost_reborn_counter: AtomicUsize::new(0), global_lock: tokio::sync::Mutex::new(()) }))
    }
}

impl Deref for SnapshotStore {
    type Target = _SnapshotStore;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl _SnapshotStore {
    pub fn snapshot_exists(&self, vc: &VectorClock) -> bool {
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        self.log.contains_key(vc)
    }

    fn snapshot_home_worker(&self, vc: &VectorClock) -> String {
        self.log.get(vc).map(|mapref| mapref.value().clone()).unwrap()
    }

    pub fn find_glb(&self, vc: &VectorClock) -> Vec<VectorClock> {
        let lbs = self.log.iter()
            .filter(|mapref| {
                let test_vc = mapref.key();
                let _cmp = test_vc.partial_cmp(vc);
                match _cmp {
                    Some(std::cmp::Ordering::Less) => true,
                    Some(std::cmp::Ordering::Equal) => true,
                    _ => false,
                }
            })
            .map(|mapref| mapref.key().clone())
            .collect::<Vec<_>>();

        lbs.iter().filter(|test_vc| {
            let res = lbs.iter().filter(|other_vc| other_vc != test_vc)
                .any(|other_vc| {
                    *test_vc < other_vc
                });

            !res
        })
        .map(|vc| vc.clone())
        .collect()
    }

    /// Returns value <= vc.
    pub async fn get(&self, key: &CacheKey, vc: &VectorClock) -> Option<CachedValue> {
        // if !self.snapshot_exists(vc) {
        //     return None;
        // }

        // // Fast path:
        // if self.snapshot_exists(vc) {
        //     let home_worker = self.snapshot_home_worker(vc);
    
        //     let store = self.store.get(&home_worker).unwrap();
        //     let store = store.read().await;
        //     if let Some(value_lattice) = store.get(key) {
        //         if let Some(val) = value_lattice.values.get(vc) {
        //             return Some(val.clone());
        //         }
        //     }
        // }

        // Slow path:
        // In all homes, find the greatest vc <= vc.
        let mut glbs = HashMap::new();

        for mapref in self.store.iter() {
            let store = mapref.value();
            let store = store.read().await;

            let Some(value_lattice) = store.get(key) else {
                continue;
            };

            let home = mapref.key().clone();

            value_lattice.values.iter().for_each(|(test_vc, _)| {
                if test_vc <= vc {
                    glbs.insert(test_vc.clone(), home.clone());
                }
            });
            
            glbs.insert(value_lattice.finalized_vc.clone(), home.clone());

        }

        // let mut to_remove = HashSet::new();
        // if glbs.len() > 1 {
        //     for (test_vc, _) in &glbs {
        //         let res = glbs.iter()
        //         .filter(|(other_vc, _)| {
        //             *other_vc != test_vc
        //         })
        //         .any(|(other_vc, _)| {
        //             test_vc < other_vc
        //         });
        //         if res {
        //             to_remove.insert(test_vc.clone());
        //         }
        //     }
        // }

        // glbs.retain(|test_vc, _| {
        //     !to_remove.contains(test_vc)
        // });


        if glbs.is_empty() {
            return None;
        }

        trace!("Glb: {:?} key: {} vc: {}", glbs, String::from_utf8(key.clone()).unwrap(), vc);

        let mut ret_val = CachedValue::new(vec![], BigInt::from_bytes_be(Sign::Plus, &default_hash()));

        for (vc, home) in glbs {
            let store = self.store.get(&home).unwrap();
            let store = store.read().await;

            let Some(value_lattice) = store.get(key) else {
                continue;
            };

            let gced_val = value_lattice.finalized_value.clone();
            let mut lattice_vals = value_lattice.values.iter()
                .filter(|(test_vc, _)| {
                    *test_vc <= &vc
                })
                .fold(CachedValue::new(vec![], BigInt::from_bytes_be(Sign::Plus, &default_hash())), |acc, (_, val)| {
                    acc.merge_immutable(val)
                });
            if let Some(gced_val) = gced_val {
                let _ = lattice_vals.merge_cached(gced_val);
            }
            
            let _ = ret_val.merge_cached(lattice_vals);
        }

        if ret_val.val_hash == BigInt::from_bytes_be(Sign::Plus, &default_hash()) {
            return None;
        }

        Some(ret_val)
    }

    pub async fn install_snapshot<T: IntoIterator<Item = (CacheKey, CachedValue)>>(&self, vc: VectorClock, worker_name: String, updates: T) {
        let store = self.store.get(&worker_name).unwrap();
        let mut store = store.write().await;
        for (key, value) in updates {
            store.entry(key)
                .or_insert(ValueLattice::new())
                .values.insert(vc.clone(), value);
        }

        if self.__ghost_log.contains(&vc) {
            self.__ghost_reborn_counter.fetch_add(1, std::sync::atomic::Ordering::Release);
        }

        
        self.log.insert(vc, worker_name);
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }

    /// Prune all snapshots < vc.
    pub async fn prune_lesser_snapshots(&self, vc: &VectorClock) {
        let all_workers = self.store.iter().map(|mapref| mapref.key().clone()).collect::<Vec<_>>();

        for home_worker in all_workers {
            let store = self.store.get(&home_worker).unwrap();
            let mut store = store.write().await;
            for mapref in store.iter_mut() {
                let val_ref = mapref.1; // mapref.value();
                let mut prune_set = HashSet::new();
                val_ref.values.iter().for_each(|(test_vc, _)| {
                    if test_vc < vc {
                        prune_set.insert(test_vc.clone());
                    }
                });

                prune_set.iter().for_each(|test_vc| {
                    let val = val_ref.values.remove(test_vc).unwrap();
                    match val_ref.finalized_value {
                        Some(ref mut finalized_value) => {
                            let _ = finalized_value.merge_cached(val.clone());
                        }
                        None => {
                            val_ref.finalized_value = Some(val.clone());
                        }
                    }

                    val_ref.finalized_vc = test_vc.clone();
                });

                
                // val_ref.values.retain(|test_vc, _| {
                //     !(test_vc < vc)
                // });
            }
        }

        

        self.log.retain(|test_vc, _| {
            let res = !(test_vc < vc);
            if !res {
                self.__ghost_log.insert(test_vc.clone());
            }
            res
        });

        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

    }

    pub async fn prune_concurrent_snapshots(&self, vcs: &Vec<&VectorClock>, upper_limits: &Vec<&VectorClock>) {
        let all_workers = self.store.iter().map(|mapref| mapref.key().clone()).collect::<Vec<_>>();

        for home_worker in all_workers {
            let store = self.store.get(&home_worker).unwrap();
            let mut store = store.write().await;
            for mapref in store.iter_mut() {
                let val_ref = mapref.1; // mapref.value();
                val_ref.values.retain(|test_vc, _| {
                    !Self::__all_concurrent(test_vc, vcs, upper_limits)
                });
            }
        }


        self.log.retain(|test_vc, _| {
            let res = !Self::__all_concurrent(test_vc, vcs, upper_limits);
            if !res {
                self.__ghost_log.insert(test_vc.clone());
            }
            res
        });

        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }

    fn __all_concurrent(vc: &VectorClock, vcs: &Vec<&VectorClock>, upper_limits: &Vec<&VectorClock>) -> bool {
        // If vc >= any of the upper limits, return false.
        let bigger_than_upper_limits = upper_limits.iter().any(|upper_limit| {
            vc >= *upper_limit
        });

        if bigger_than_upper_limits {
            return false;
        }
        
        vcs.iter().all(|test_vc| {
            !(vc <= *test_vc || *test_vc <= vc)
        })
    }

    pub fn size(&self) -> usize {
        self.log.len()
    }

    pub fn ghost_reborn_counter(&self) -> usize {
        self.__ghost_reborn_counter.load(std::sync::atomic::Ordering::Acquire)
    }
}