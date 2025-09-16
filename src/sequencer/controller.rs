use std::sync::Arc;

use log::{debug, info, warn};
use tokio::sync::Mutex;
use prost::Message as _;

use crate::{config::AtomicConfig, proto::{client::{ProtoClientReply, ProtoClientRequest, ProtoTransactionReceipt}, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType, ProtoTransactionPhase, ProtoTransactionResult}, rpc::ProtoPayload}, rpc::{client::PinnedClient, server::{LatencyProfile, MsgAckChan}, PinnedMessage, SenderType}, utils::channel::Receiver, worker::{block_sequencer::VectorClock, cache_manager::CacheKey}};

pub enum ControllerCommand {
    BlockAllWorkers,
    BlockChosenWorkers(Vec<String>),
    UnblockAllWorkers,

    /// This is used to grant release-consistent locks to a worker.
    BlockingLockAcquire(CacheKey, SenderType, VectorClock, MsgAckChan, u64),
    UnlockAck(MsgAckChan, u64),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
enum BlockingState {
    Unblocked = 0,
    Blocked = 1,
}

pub struct Controller {
    config: AtomicConfig,
    client: PinnedClient,
    command_rx1: Receiver<ControllerCommand>,
    command_rx2: Receiver<ControllerCommand>,

    blocking_state: BlockingState,

    __client_tag_counter: u64,
}

impl Controller {
    pub fn new(config: AtomicConfig, client: PinnedClient, command_rx1: Receiver<ControllerCommand>, command_rx2: Receiver<ControllerCommand>) -> Self {
        Self {
            config,
            client,
            command_rx1,
            command_rx2,
            blocking_state: BlockingState::Unblocked,
            __client_tag_counter: 0,
        }
    }

    pub async fn run(controller: Arc<Mutex<Self>>) {
        let mut controller = controller.lock().await;
        
        while let Ok(()) = controller.worker().await {

        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let cmd = tokio::select! {
            Some(cmd) = self.command_rx1.recv() => {
                cmd
            }
            Some(cmd) = self.command_rx2.recv() => {
                cmd
            }
        };

        match cmd {
            ControllerCommand::BlockAllWorkers => {
                self.block_all_workers().await;
            }
            ControllerCommand::BlockChosenWorkers(workers) => {
                self.block_chosen_workers(workers).await;
            }
            ControllerCommand::UnblockAllWorkers => {
                self.unblock_all_workers().await;
            }
            ControllerCommand::BlockingLockAcquire(key, sender, vc, ack_chan, client_tag) => {
                self.blocking_lock_acquire(key, sender, vc, ack_chan, client_tag).await;
            }
            ControllerCommand::UnlockAck(ack_chan, client_tag) => {
                self.unlock_ack(ack_chan, client_tag).await;
            }
        }

        Ok(())
    }

    async fn unlock_ack(&mut self, ack_chan: MsgAckChan, client_tag: u64) {
        let reply = ProtoClientReply {
            client_tag,
            reply: Some(crate::proto::client::proto_client_reply::Reply::Receipt(ProtoTransactionReceipt {
                req_digest: vec![],
                block_n: 0,
                tx_n: 0,
                results: Some(ProtoTransactionResult {
                    result: vec![ProtoTransactionOpResult { success: true, values: vec![vec![5u8; 4096]] }],
                }),
                await_byz_response: false,
                byz_responses: vec![],
            })),
        };

        let reply_ser = reply.encode_to_vec();
        let _sz = reply_ser.len();
        let reply_msg = PinnedMessage::from(reply_ser, _sz, crate::rpc::SenderType::Anon);
        let _ = ack_chan.send((reply_msg, LatencyProfile::new())).await;

    }

    async fn _send_request_to_all_workers(&mut self, tx: ProtoTransaction) {
        let node_list = self.get_node_list();
        self._send_request_to_chosen_workers(tx, node_list).await;
    }

    async fn _send_request_to_chosen_workers(&mut self, tx: ProtoTransaction, node_list: Vec<String>) {
        self.__client_tag_counter += 1;
        let request = ProtoClientRequest {
            tx: Some(tx),
            origin: self.config.get().net_config.name.clone(),
            sig: vec![0u8; 1],
            client_tag: self.__client_tag_counter,
        };

        let request = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(request)),
        };

        let buf = request.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        for node in node_list {
            let resp = PinnedClient::send(&self.client, &node, request.as_ref()).await;

            if let Err(e) = &resp {
                warn!("Failed to send request to node {}: {:?}", node, e);
                continue;
            }
        }
    }

    async fn block_chosen_workers(&mut self, workers: Vec<String>) {
        // if self.blocking_state == BlockingState::Blocked {
        //     return;
        // }
        debug!("Blocking workers.");
        self.blocking_state = BlockingState::Blocked;

        let tx = ProtoTransaction {
            on_receive: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::BlockIndefinitely as i32,
                    operands: vec![],
                }],
            }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        self._send_request_to_chosen_workers(tx, workers).await;
    }

    async fn block_all_workers(&mut self) {
        let node_list = self.get_node_list();
        self.block_chosen_workers(node_list).await;
    }


    fn get_node_list(&self) -> Vec<String> {
        // There must be a better way to do this.
        self.config.get().net_config.nodes.iter()
            .filter(|(name, _)| name.starts_with("node"))
            .map(|(name, _)| name.clone())
            .collect()
    }

    async fn unblock_all_workers(&mut self) {
        if self.blocking_state == BlockingState::Unblocked {
            return;
        }
        self.blocking_state = BlockingState::Unblocked;
        debug!("Unblocking workers.");

        let tx = ProtoTransaction {
            on_receive: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Unblock as i32,
                    operands: vec![],
                }],
            }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        self._send_request_to_all_workers(tx).await;

    }

    async fn blocking_lock_acquire(&mut self, key: CacheKey, sender: SenderType, vc: VectorClock, ack_chan: MsgAckChan, client_tag: u64) {
        // if self.blocking_state == BlockingState::Blocked {
        //     return;
        // }
        info!("Blocking worker {:?} till VC {} to acquire lock on {}.", sender, vc, String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone())));

        let reply = ProtoClientReply {
            client_tag,
            reply: Some(crate::proto::client::proto_client_reply::Reply::Receipt(ProtoTransactionReceipt {
                req_digest: vec![],
                block_n: 0,
                tx_n: 0,
                results: Some(ProtoTransactionResult {
                    result: vec![ProtoTransactionOpResult { success: true, values: vec![vc.serialize().encode_to_vec()] }],
                }),
                await_byz_response: false,
                byz_responses: vec![],
            })),
        };

        let reply_ser = reply.encode_to_vec();
        let _sz = reply_ser.len();
        let reply_msg = PinnedMessage::from(reply_ser, _sz, crate::rpc::SenderType::Anon);
        let _ = ack_chan.send((reply_msg, LatencyProfile::new())).await;
    }
}