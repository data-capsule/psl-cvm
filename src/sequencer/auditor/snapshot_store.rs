use std::{ops::Deref, sync::{atomic::AtomicUsize, Arc}};
use dashmap::{DashMap, DashSet};
use crate::worker::{block_sequencer::VectorClock, cache_manager::{CacheKey, CachedValue}};


struct ValueLattice {
    values: DashMap<VectorClock, CachedValue>,
}
pub struct _SnapshotStore {
    store: DashMap<CacheKey, ValueLattice>,
    /// All VCs currently installed.
    log: DashSet<VectorClock>,
    /// All VCs that have been garbage collected.
    __ghost_log: DashSet<VectorClock>,
    __ghost_reborn_counter: AtomicUsize,
}


/// This will be shared across all auditors.
#[derive(Clone)]
pub struct SnapshotStore(Arc<_SnapshotStore>);

impl SnapshotStore {
    pub fn new() -> Self {
        let log = DashSet::new();
        log.insert(VectorClock::new());
        Self(Arc::new(_SnapshotStore { store: DashMap::new(), log, __ghost_log: DashSet::new(), __ghost_reborn_counter: AtomicUsize::new(0) }))
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
        self.log.contains(vc)
    }

    pub fn find_glb(&self, vc: &VectorClock) -> Vec<VectorClock> {
        let lbs = self.log.iter()
            .filter(|test_vc| {
                let _cmp = test_vc.partial_cmp(vc);
                match _cmp {
                    Some(std::cmp::Ordering::Less) => true,
                    Some(std::cmp::Ordering::Equal) => true,
                    _ => false,
                }
            })
            .map(|vc| vc.clone())
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
    pub fn get(&self, key: &CacheKey, vc: &VectorClock) -> Option<CachedValue> {
        if !self.snapshot_exists(vc) {
            return None;
        }

        let Some(value_lattice) = self.store.get(key) else {
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
            let test_vc = mapref.key();
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

    pub fn install_snapshot<T: IntoIterator<Item = (CacheKey, CachedValue)>>(&self, vc: VectorClock, updates: T) {
        for (key, value) in updates {
            self.store.entry(key)
                .or_insert(ValueLattice { values: DashMap::new() })
                .values.insert(vc.clone(), value);
        }

        if self.__ghost_log.contains(&vc) {
            self.__ghost_reborn_counter.fetch_add(1, std::sync::atomic::Ordering::Release);
        }

        
        self.log.insert(vc);
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }

    /// Prune all snapshots <= vc.
    pub fn prune_lesser_snapshots(&self, vc: &VectorClock) {
        for mapref in self.store.iter_mut() {
            let val_ref = mapref.value();
            val_ref.values.retain(|test_vc, _| {
                !(test_vc < vc)
            });
        }

        self.log.retain(|test_vc| {
            let res = !(test_vc < vc);
            if !res {
                self.__ghost_log.insert(test_vc.clone());
            }
            res
        });

        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

    }

    pub fn prune_concurrent_snapshots(&self, vcs: &Vec<&VectorClock>, upper_limits: &Vec<&VectorClock>) {
        for mapref in self.store.iter_mut() {
            let val_ref = mapref.value();
            val_ref.values.retain(|test_vc, _| {
                !Self::__all_concurrent(test_vc, vcs, upper_limits)
            });
        }

        self.log.retain(|test_vc| {
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