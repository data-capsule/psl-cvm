use std::{collections::{HashMap, HashSet, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::{error, info};
use tokio::sync::{mpsc::UnboundedReceiver, Mutex};
use prost::Message as _;

use crate::{config::AtomicConfig, proto::{client::ProtoClientRequest, consensus::{ProtoHeartbeat, ProtoVectorClock}}, rpc::{server::MsgAckChan, SenderType}, sequencer::controller::ControllerCommand, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::{block_sequencer::VectorClock, cache_manager::CacheKey}};

#[derive(Debug, Clone)]
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
    command_rx: UnboundedReceiver<(LockServerCommand, SenderType, MsgAckChan, u64)>,
    heartbeat_rx: UnboundedReceiver<(ProtoHeartbeat, SenderType)>,
    heartbeat_vc: HashMap<String, VectorClock>,

    controller_tx: Sender<ControllerCommand>,

    lock_map: HashMap<CacheKey, LockState>,
    lock_request_buffer: HashMap<CacheKey, VecDeque<(LockServerCommand, SenderType, MsgAckChan, u64)>>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl LockServer {
    pub fn new(config: AtomicConfig, heartbeat_rx: UnboundedReceiver<(ProtoHeartbeat, SenderType)>, command_rx: UnboundedReceiver<(LockServerCommand, SenderType, MsgAckChan, u64)>, controller_tx: Sender<ControllerCommand>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config,
            command_rx,
            heartbeat_rx,
            heartbeat_vc: HashMap::new(),
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
        let heartbeat_rx_len = self.heartbeat_rx.len();
        if heartbeat_rx_len > 0 {
            let mut heartbeats = Vec::new();
            self.heartbeat_rx.recv_many(&mut heartbeats, heartbeat_rx_len).await;
            let _ = self.process_heartbeats(heartbeats).await;
            return Ok(());
        }
        let command_rx_len = self.command_rx.len();
        if command_rx_len > 0 {
            let mut cmds = Vec::new();
            self.command_rx.recv_many(&mut cmds, command_rx_len).await;
            let _ = self.process_cmd(cmds).await;
            return Ok(());
        }

        tokio::select! {
            Some((cmds, sender, ack_chan, client_tag)) = self.command_rx.recv() => {
                let _ = self.process_cmd(vec![(cmds, sender, ack_chan, client_tag)]).await;
            }
            Some((proto_heartbeat, sender)) = self.heartbeat_rx.recv() => {
                let _ = self.process_heartbeats(vec![(proto_heartbeat, sender)]).await;
            }
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            }
        }

        Ok(())
    }

    async fn process_heartbeats(&mut self, heartbeats: Vec<(ProtoHeartbeat, SenderType)>) -> Result<(), ()> {
        for (proto_heartbeat, sender) in heartbeats {
            let proto_heartbeat_vc = proto_heartbeat.vector_clock;
            let _name = sender.to_name_and_sub_id().0;
            self.heartbeat_vc.insert(_name, VectorClock::from(proto_heartbeat_vc));
        }
        let lock_keys = self.lock_request_buffer.iter()
            .filter_map(|(key, requests)| {
                if requests.len() > 0 {
                    Some(key.clone())
                } else {
                    None
                }
            }).collect::<HashSet<_>>();
        for key in lock_keys {
            loop {
                if !self.maybe_grant_locks(key.clone()).await {
                    break;
                }
            }
        }
        
        Ok(())

    }

    async fn log_stats(&self) {
        let locked_keys = self.lock_map.iter().filter(|(_, lock_state)| !matches!(lock_state.locker, LockType::Unlocked)).count();
        let buffered_requests = self.lock_request_buffer.iter().map(|(_, requests)| requests.len()).sum::<usize>();
        info!("Total locks: {}, Total active locks: {}, Buffered requests: {}",
            self.lock_map.len(),
            locked_keys,
            buffered_requests
        );

        if buffered_requests <= 10 {
            log::warn!("Buffered requests: {:?}", self.lock_request_buffer.iter()
                .filter(|(_, requests)| requests.len() > 0)
                .map(|(key, requests)| (String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone())), 
                    self.lock_map.get(key).unwrap().locker.clone(),
                    requests.iter().map(|(_, sender, _, _)| sender.clone()).collect::<Vec<_>>()))
                .collect::<Vec<_>>()
            );

        }



    }

    async fn process_cmd(&mut self, cmds: Vec<(LockServerCommand, SenderType, MsgAckChan, u64)>) -> Result<(), ()> {
        let mut locking_candidates = HashSet::new();
        
        for (cmd, sender, ack_chan, client_tag) in cmds {
            let key = match cmd {
                LockServerCommand::ReleaseLock(key, vc) => {
                    self.release_lock(key.clone(), sender.clone(), vc);
                    self.controller_tx.send(ControllerCommand::UnlockAck(ack_chan.clone(), client_tag)).await.unwrap();
                    key.clone()
                },
                LockServerCommand::AcquireReadLock(ref key) | LockServerCommand::AcquireWriteLock(ref key) => {
                    let _key = key.clone();
                    self.buffer_lock_request(cmd, sender.clone(), ack_chan.clone(), client_tag);
                    _key
                }
            };

            locking_candidates.insert(key.clone());
        }

        for key in locking_candidates.drain() {
            loop {
                if !self.maybe_grant_locks(key.clone()).await {
                    break;
                }
            }
        }


        Ok(())
    }


    /// Precondition:
    /// - is_acquirable(key) == true
    /// - => lock_map.get(key).is_none() or lock_map.get(key).locker == LockType::Unlocked or lock_map.get(key).locker == LockType::Read(_)
    async fn acquire_read_lock(&mut self, key: CacheKey, sender: SenderType, ack_chan: MsgAckChan, client_tag: u64) {
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

        self.notify(key, sender, min_vc, ack_chan, client_tag).await;

    }


    /// Precondition:
    /// - is_acquirable(key) == true 
    /// - => lock_map.get(key).is_none() or lock_map.get(key).locker == LockType::Unlocked
    async fn acquire_write_lock(&mut self, key: CacheKey, sender: SenderType, ack_chan: MsgAckChan, client_tag: u64) {
        let lock_state = self.lock_map.entry(key.clone()).or_insert(LockState {
            locker: LockType::Unlocked,
            min_vc: VectorClock::new(),
        });

        lock_state.locker = LockType::Write(sender.clone());


        let min_vc = lock_state.min_vc.clone();
        self.notify(key, sender, min_vc, ack_chan, client_tag).await;
    }


    /// Ref: src/worker/app.rs:L263-264
    /// The nonblocking client has id + 1000 of the blocking client.
    /// The blocking client sends lock requests, the nonblocking client sends release requests.
    fn convert_to_locker_sender(sender: SenderType) -> SenderType {
        // match sender {
        //     SenderType::Auth(sender, id) => SenderType::Auth(sender, id - 1000),
        //     _ => sender,
        // }
        sender
    }

    fn release_lock(&mut self, key: CacheKey, sender: SenderType, vc: VectorClock) {
        let sender = Self::convert_to_locker_sender(sender);
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

    fn buffer_lock_request(&mut self, cmd: LockServerCommand, sender: SenderType, ack_chan: MsgAckChan, client_tag: u64) {
        let key = match &cmd {
            LockServerCommand::AcquireReadLock(key) | LockServerCommand::AcquireWriteLock(key) => {
                key.clone()
            },
            _ => {
                unreachable!();
            }
        };

        self.lock_request_buffer.entry(key).or_insert(VecDeque::new()).push_back((cmd, sender, ack_chan, client_tag));
    }

    async fn maybe_grant_locks(&mut self, key: CacheKey) -> bool {
        let Some(pending_requests) = self.lock_request_buffer.get_mut(&key) else {
            return false;
        };


        let (cmd, sender, ack_chan, client_tag) = if pending_requests.len() > 0 {
            if !Self::is_acquirable(&self.lock_map, pending_requests.front().unwrap(), &self.heartbeat_vc) {
                return false;
            }
            let Some((cmd, sender, ack_chan, client_tag)) = pending_requests.pop_front() else {
                unreachable!();
            };

            (cmd, sender, ack_chan, client_tag)
        } else {
            return false;
        };

        match cmd {
            LockServerCommand::AcquireReadLock(key) => {
                self.acquire_read_lock(key, sender, ack_chan, client_tag).await;
            },
            LockServerCommand::AcquireWriteLock(key) => {
                self.acquire_write_lock(key, sender, ack_chan, client_tag).await;
            },
            _ => {
                unreachable!();
            }
        };

        true
    }

    /// If it is a write lock request, locktype must be Unlocked.
    /// If it is a read lock request, locktype must be Unlocked or Read(_).
    fn is_acquirable(lock_map: &HashMap<CacheKey, LockState>, cmd: &(LockServerCommand, SenderType, MsgAckChan, u64), heartbeat_vc: &HashMap<String, VectorClock>) -> bool {
        // let __default_vc = VectorClock::new();
        // let heartbeat_vc = __default_vc; // heartbeat_vc.get(&cmd.1.to_name_and_sub_id().0).unwrap_or(&__default_vc);
        let key = match &cmd.0 {
            LockServerCommand::AcquireReadLock(key) | LockServerCommand::AcquireWriteLock(key) => {
                key
            },
            _ => {
                unreachable!();
            }
        };
        
        let (lock_type, min_vc) = match lock_map.get(key) {
            Some(lock_state) => (&lock_state.locker, &lock_state.min_vc),
            None => (&LockType::Unlocked, &VectorClock::new()),
        };

        match &cmd.0 {
            LockServerCommand::AcquireReadLock(_) => {
                matches!(lock_type, LockType::Unlocked | LockType::Read(_)) //  && min_vc <= heartbeat_vc
            }
            LockServerCommand::AcquireWriteLock(_) => {
                matches!(lock_type, LockType::Unlocked) // && min_vc <= heartbeat_vc
            }
            _ => {
                unreachable!();
            }
        }
    }

    async fn notify(&mut self, key: CacheKey, sender: SenderType, vc: VectorClock, ack_chan: MsgAckChan, client_tag: u64) {
        let cmd = ControllerCommand::BlockingLockAcquire(key, sender, vc, ack_chan, client_tag);
        self.controller_tx.send(cmd).await.unwrap();
    }


    /// Semantics are as follows:
    /// - Transaction must have an on_crash_commit phase.
    /// - For all ops in on_crash_commit,
    ///     READ request is considered as Read Lock request,
    ///     WRITE request is considered as Write Lock request,
    ///     UNBLOCK request is considered as Release request,
    /// - Ordering of the locks matter. Otherwise the system will deadlock.
    /// - This function will NOT sort the lock names.
    /// - Returns whether the client expects a reply.
    pub fn to_lock_server_command(client_request: ProtoClientRequest) -> (bool, Vec<LockServerCommand>) {
        let Some(tx) = client_request.tx else {
            return (false, vec![]);
        };

        let Some(phase) = tx.on_crash_commit else {
            return (false, vec![]);
        };

        if phase.ops.is_empty() {
            return (false, vec![]);
        }

        let mut only_release_commands = true;

        let res = phase.ops.iter()
            .filter_map(|op| {
                match op.op_type() {
                    crate::proto::execution::ProtoTransactionOpType::Read | crate::proto::execution::ProtoTransactionOpType::Write => {
                        if op.operands.len() != 1 {
                            return None;
                        }

                        let key = op.operands[0].clone();
                        only_release_commands = false;

                        if op.op_type() == crate::proto::execution::ProtoTransactionOpType::Read {
                            Some(LockServerCommand::AcquireReadLock(key))
                        } else {
                            Some(LockServerCommand::AcquireWriteLock(key))
                        }

                    },
                    crate::proto::execution::ProtoTransactionOpType::Unblock => {
                        if op.operands.len() != 2 {
                            return None;
                        }

                        let key = op.operands[0].clone();
                        let vc = ProtoVectorClock::decode(op.operands[1].as_slice()).unwrap();
                        Some(LockServerCommand::ReleaseLock(key, VectorClock::from(Some(vc))))
                    },
                    _ => {
                        return None;
                    }
                }
            })
            .collect();

        (only_release_commands, res)
    }
}