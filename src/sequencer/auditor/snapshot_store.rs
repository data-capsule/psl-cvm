use std::{ops::Deref, sync::Arc};
use dashmap::{DashMap, DashSet};
use crate::worker::{block_sequencer::VectorClock, cache_manager::{CacheKey, CachedValue}};


struct ValueLattice {
    values: DashMap<VectorClock, CachedValue>,
}
pub struct _SnapshotStore {
    store: DashMap<CacheKey, ValueLattice>,
    log: DashSet<VectorClock>,
}


/// This will be shared across all auditors.
#[derive(Clone)]
pub struct SnapshotStore(Arc<_SnapshotStore>);

impl SnapshotStore {
    pub fn new() -> Self {
        Self(Arc::new(_SnapshotStore { store: DashMap::new(), log: DashSet::new() }))
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

        
        self.log.insert(vc);
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }

    /// Prune all snapshots <= vc.
    pub fn prune_snapshots(&self, vc: &VectorClock) {
        for mapref in self.store.iter_mut() {
            let val_ref = mapref.value();
            val_ref.values.retain(|test_vc, _| {
                !(test_vc <= vc)
            });
        }

        self.log.retain(|test_vc| {
            !(test_vc <= vc)
        });

        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

    }

    pub fn size(&self) -> usize {
        self.log.len()
    }
}