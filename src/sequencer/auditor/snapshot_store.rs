use std::{collections::HashMap, ops::Deref, sync::{atomic::AtomicUsize, Arc}};
use tokio::sync::RwLock;
use dashmap::{DashMap, DashSet};
use log::warn;
use crate::worker::{block_sequencer::VectorClock, cache_manager::{CacheKey, CachedValue}};


pub(super) struct ValueLattice {
    values: HashMap<VectorClock, CachedValue>,
}

impl ValueLattice {
    fn new() -> Self {
        Self { values: HashMap::new() }
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
}


/// This will be shared across all auditors.
#[derive(Clone)]
pub struct SnapshotStore(Arc<_SnapshotStore>);

impl SnapshotStore {
    pub fn new(worker_names: Vec<String>) -> Self {
        let log = DashMap::new();
        let store = worker_names.iter().map(|name| (name.clone(), RwLock::new(HashMap::new()))).collect();

        log.insert(VectorClock::new(), worker_names[0].clone());
        Self(Arc::new(_SnapshotStore { store, log, __ghost_log: DashSet::new(), __ghost_reborn_counter: AtomicUsize::new(0) }))
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
        if !self.snapshot_exists(vc) {
            return None;
        }

        let home_worker = self.snapshot_home_worker(vc);

        let store = self.store.get(&home_worker).unwrap();
        let store = store.read().await;
        let Some(value_lattice) = store.get(key) else {
            return None;
        };

        // Fast path:
        if let Some(val) = value_lattice.values.get(vc) {
            return Some(val.clone());
        }

        // Slow path
        // Find the greatest vc <= vc.
        let mut glb = None;
        for mapref in value_lattice.values.iter() {
            let test_vc = mapref.0; // mapref.key();
            if test_vc <= vc {
                if glb.is_none() {
                    glb = Some(test_vc.clone());
                } else {
                    let _glb = glb.as_ref().unwrap();
                    if test_vc > _glb {
                        glb = Some(test_vc.clone());
                    }
                }
            }
        }

        let glb = glb.unwrap();
        value_lattice.values.get(&glb).map(|val| val.clone())
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
                val_ref.values.retain(|test_vc, _| {
                    !(test_vc < vc)
                });
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