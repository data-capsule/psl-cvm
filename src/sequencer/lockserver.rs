use std::{collections::{HashMap, HashSet, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::info;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, proto::client::ProtoClientRequest, rpc::SenderType, sequencer::controller::ControllerCommand, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::VectorClock, cache_manager::CacheKey}};

pub enum LockServerCommand {
    AcquireReadLock(CacheKey),
    AcquireWriteLock(CacheKey),
    ReleaseLock(CacheKey, VectorClock),
}

#[derive(Debug, Clone)]
enum LockType {
    Unlocked,
    Read(HashSet<SenderType>),
    Write(SenderType),
}

#[derive(Debug, Clone)]
struct LockState {
    locker: LockType,
    min_vc: VectorClock,
}

pub struct LockServer {
    config: AtomicConfig,
    command_rx: Receiver<(Vec<LockServerCommand>, SenderType)>,
    controller_tx: Sender<ControllerCommand>,

    lock_map: HashMap<CacheKey, LockState>,
    lock_request_buffer: HashMap<CacheKey, VecDeque<(LockServerCommand, SenderType)>>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl LockServer {
    pub fn new(config: AtomicConfig, command_rx: Receiver<(Vec<LockServerCommand>, SenderType)>, controller_tx: Sender<ControllerCommand>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config,
            command_rx,
            controller_tx,
            lock_map: HashMap::new(),
            lock_request_buffer: HashMap::new(),
            log_timer,
        }
    }

    pub async fn run(lock_server: Arc<Mutex<Self>>) {
        let mut lock_server = lock_server.lock().await;
        lock_server.log_timer.run().await;
        while let Ok(()) = lock_server.worker().await {
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            Some((cmds, sender)) = self.command_rx.recv() => {
                let _ = self.process_cmd(cmds, sender).await;
            }
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            }
        }

        Ok(())
    }

    async fn log_stats(&self) {
        let locked_keys = self.lock_map.iter().filter(|(_, lock_state)| !matches!(lock_state.locker, LockType::Unlocked)).count();
        info!("Total locks: {}, Total active locks: {}, Buffered requests: {}",
            self.lock_map.len(),
            locked_keys,
            self.lock_request_buffer.len()
        );

    }

    async fn process_cmd(&mut self, cmds: Vec<LockServerCommand>, sender: SenderType) -> Result<(), ()> {
        for cmd in cmds {
            let key = match cmd {
                LockServerCommand::ReleaseLock(key, vc) => {
                    self.release_lock(key.clone(), sender.clone(), vc).await;
                    key.clone()
                },
                LockServerCommand::AcquireReadLock(ref key) | LockServerCommand::AcquireWriteLock(ref key) => {
                    let _key = key.clone();
                    self.buffer_lock_request(cmd, sender.clone()).await;
                    _key
                }
            };
            self.maybe_grant_locks(key).await;
        }


        Ok(())
    }


    /// Precondition:
    /// - is_acquirable(key) == true
    /// - => lock_map.get(key).is_none() or lock_map.get(key).locker == LockType::Unlocked or lock_map.get(key).locker == LockType::Read(_)
    async fn acquire_read_lock(&mut self, key: CacheKey, sender: SenderType) {
        let lock_state = self.lock_map.entry(key.clone()).or_insert(LockState {
            locker: LockType::Unlocked,
            min_vc: VectorClock::new(),
        });
        

        match &mut lock_state.locker {
            LockType::Unlocked => {
                let mut readers = HashSet::new();
                readers.insert(sender.clone());
                lock_state.locker = LockType::Read(readers);
            },
            LockType::Read(readers) => {
                readers.insert(sender.clone());
            },
            _ => {
                unreachable!();
            }
        }

        let min_vc = lock_state.min_vc.clone();

        self.notify(key, sender, min_vc).await;

    }


    /// Precondition:
    /// - is_acquirable(key) == true 
    /// - => lock_map.get(key).is_none() or lock_map.get(key).locker == LockType::Unlocked
    async fn acquire_write_lock(&mut self, key: CacheKey, sender: SenderType) {
        let lock_state = self.lock_map.entry(key.clone()).or_insert(LockState {
            locker: LockType::Unlocked,
            min_vc: VectorClock::new(),
        });

        lock_state.locker = LockType::Write(sender.clone());


        let min_vc = lock_state.min_vc.clone();
        self.notify(key, sender, min_vc).await;
    }

    async fn release_lock(&mut self, key: CacheKey, sender: SenderType, vc: VectorClock) {
        let Some(lock_state) = self.lock_map.get_mut(&key) else {
            return;
        };

        let (must_update_vc, must_reset_locktype) = match &mut lock_state.locker {
            LockType::Unlocked => {
                // Nothing to do here.
                (false, true)
            },
            LockType::Read(readers) => {
                let reader_existed = readers.remove(&sender);
                let readers_empty = readers.is_empty();
                (reader_existed, readers_empty)
            },
            LockType::Write(current_locker) => {
                if current_locker == &sender {
                    (true, true)
                } else {
                    (false, false)
                }
            },
        };

        if must_update_vc {
            lock_state.min_vc = vc;
        }

        if must_reset_locktype {
            lock_state.locker = LockType::Unlocked;
        }
    }

    async fn buffer_lock_request(&mut self, cmd: LockServerCommand, sender: SenderType) {
        let key = match &cmd {
            LockServerCommand::AcquireReadLock(key) | LockServerCommand::AcquireWriteLock(key) => {
                key.clone()
            },
            _ => {
                unreachable!();
            }
        };

        self.lock_request_buffer.entry(key).or_insert(VecDeque::new()).push_back((cmd, sender));
    }

    async fn maybe_grant_locks(&mut self, key: CacheKey) {
        let mut applied_lock_requests = Vec::new();
        let Some(pending_requests) = self.lock_request_buffer.get_mut(&key) else {
            return;
        };

        if pending_requests.is_empty() {
            return;
        }

        while pending_requests.len() > 0 {
            if !Self::is_acquirable(&self.lock_map, pending_requests.front().unwrap()) {
                break;
            }
            let Some((cmd, sender)) = pending_requests.pop_front() else {
                unreachable!();
            };

            applied_lock_requests.push((cmd, sender));
        }

        for (cmd, sender) in applied_lock_requests.drain(..) {
            match cmd {
                LockServerCommand::AcquireReadLock(key) => {
                    self.acquire_read_lock(key, sender).await;
                },
                LockServerCommand::AcquireWriteLock(key) => {
                    self.acquire_write_lock(key, sender).await;
                },
                _ => {
                    unreachable!();
                }
            };
        }
    }


    /// If it is a write lock request, locktype must be Unlocked.
    /// If it is a read lock request, locktype must be Unlocked or Read(_).
    fn is_acquirable(lock_map: &HashMap<CacheKey, LockState>, cmd: &(LockServerCommand, SenderType)) -> bool {
        let key = match &cmd.0 {
            LockServerCommand::AcquireReadLock(key) | LockServerCommand::AcquireWriteLock(key) => {
                key
            },
            _ => {
                unreachable!();
            }
        };
        
        let lock_type = match lock_map.get(key) {
            Some(lock_state) => &lock_state.locker,
            None => &LockType::Unlocked,
        };

        match &cmd.0 {
            LockServerCommand::AcquireReadLock(_) => {
                matches!(lock_type, LockType::Unlocked | LockType::Read(_))
            }
            LockServerCommand::AcquireWriteLock(_) => {
                matches!(lock_type, LockType::Unlocked)
            }
            _ => {
                unreachable!();
            }
        }
    }

    async fn notify(&mut self, key: CacheKey, sender: SenderType, vc: VectorClock) {
        let cmd = ControllerCommand::BlockingLockAcquire(key, sender, vc);
        self.controller_tx.send(cmd).await.unwrap();
    }


    /// Semantics are as follows:
    /// - Transaction must have an on_crash_commit phase.
    /// - For all ops in on_crash_commit, READ request is considered as Read Lock request, and WRITE request is considered as Write Lock request.
    /// - Ordering of the locks matter. Otherwise the system will deadlock.
    /// - This function will NOT sort the lock names.
    pub fn to_lock_server_command(client_request: ProtoClientRequest) -> Vec<LockServerCommand> {
        let Some(tx) = client_request.tx else {
            return vec![];
        };

        let Some(phase) = tx.on_crash_commit else {
            return vec![];
        };

        phase.ops.iter()
            .filter_map(|op| {
                match op.op_type() {
                    crate::proto::execution::ProtoTransactionOpType::Read | crate::proto::execution::ProtoTransactionOpType::Write => {
                        if op.operands.len() != 1 {
                            return None;
                        }

                        let key = op.operands[0].clone();

                        if op.op_type() == crate::proto::execution::ProtoTransactionOpType::Read {
                            Some(LockServerCommand::AcquireReadLock(key))
                        } else {
                            Some(LockServerCommand::AcquireWriteLock(key))
                        }
                    },
                    _ => {
                        return None;
                    }
                }
            })
            .collect()
    }
}