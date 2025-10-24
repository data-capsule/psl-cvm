use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
#[cfg(feature = "nimble")]
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, oneshot};
use tokio::sync::Mutex;

#[cfg(feature = "nimble")]
use crate::{crypto::HashType, proto::client::ProtoClientRequest, rpc::{client::PinnedClient, PinnedMessage}};


use crate::{config::AtomicPSLWorkerConfig, crypto::{CachedBlock, CryptoServiceConnector}, proto::consensus::ProtoVote, rpc::SenderType, utils::channel::{make_channel, Receiver, Sender}};

pub type VoteWithSender = (SenderType, ProtoVote);

/// ```
///                                                          ------------------------------------
///                   Vote                      |--------->  |Block Broadcaster to Other Workers|
///                    |-------------------|    |            ------------------------------------
///                                        |    |
///                                        v    |
/// ------------------------------       ---------       -----------
/// |Block Broadcaster to Storage| ----> |Staging| ----> |LogServer|
/// ------------------------------       ---------       -----------
///                                         |            ----------------------
///                                         |----------> |Client Reply Handler|
///                                                      ----------------------
/// ```
pub struct Staging {
    config: AtomicPSLWorkerConfig,
    chain_id: u64,
    crypto: CryptoServiceConnector,

    vote_rx: Receiver<VoteWithSender>,
    block_rx: Receiver<CachedBlock>,

    block_broadcaster_to_other_workers_tx: Sender<u64>,
    logserver_tx: Sender<(SenderType, CachedBlock)>,
    client_reply_tx: tokio::sync::broadcast::Sender<u64>,

    vote_buffer: HashMap<u64, Vec<VoteWithSender>>,
    block_buffer: Vec<CachedBlock>,

    commit_index: u64,
    gc_tx: Sender<(SenderType, u64)>,

    // #[cfg(feature = "nimble")]
    // nimble_client_tx: Sender<(Sender<()>, HashType)>,

    #[cfg(feature = "nimble")]
    nimble_client: PinnedClient,

    #[cfg(feature = "nimble")]
    nimble_client_tag: u64,

    #[cfg(feature = "nimble")]
    nimble_request_sender_tx: UnboundedSender<PinnedMessage>,

    #[cfg(feature = "nimble")]
    nimble_request_sender_rx: Option<UnboundedReceiver<PinnedMessage>>,
}

impl Staging {
    pub fn new(config: AtomicPSLWorkerConfig, chain_id: u64, crypto: CryptoServiceConnector,
        vote_rx: Receiver<VoteWithSender>, block_rx: Receiver<CachedBlock>,
        block_broadcaster_to_other_workers_tx: Sender<u64>, logserver_tx: Sender<(SenderType, CachedBlock)>,
        client_reply_tx: tokio::sync::broadcast::Sender<u64>, gc_tx: Sender<(SenderType, u64)>,

        #[cfg(feature = "nimble")]
        nimble_client: PinnedClient,
    ) -> Self {

        #[cfg(feature = "nimble")]
        let (nimble_request_sender_tx, nimble_request_sender_rx) = unbounded_channel();

        Self {
            config,
            chain_id,
            crypto,
            vote_rx,
            block_rx,
            block_broadcaster_to_other_workers_tx,
            logserver_tx,
            client_reply_tx,

            vote_buffer: HashMap::new(),
            block_buffer: Vec::new(),

            commit_index: 0,
            gc_tx,

            #[cfg(feature = "nimble")]
            nimble_client,

            #[cfg(feature = "nimble")]
            nimble_client_tag: 0,

            #[cfg(feature = "nimble")]
            nimble_request_sender_tx,

            #[cfg(feature = "nimble")]
            nimble_request_sender_rx: Some(nimble_request_sender_rx),
        }
    }

    pub async fn run(staging: Arc<Mutex<Self>>) {
        let mut staging = staging.lock().await;
        #[cfg(feature = "nimble")]
        {
            

            let mut request_rx = staging.nimble_request_sender_rx.take().unwrap();
            let client = staging.nimble_client.clone();
            let gc_tx = staging.gc_tx.clone();
            let me = SenderType::Auth(staging.config.get().net_config.name.clone(), staging.chain_id);
            let block_broadcaster_to_other_workers_tx = staging.block_broadcaster_to_other_workers_tx.clone();
            let client_reply_tx = staging.client_reply_tx.clone();
            tokio::spawn(async move {
                loop {
                    use prost::Message as _;
                    use crate::proto::client::ProtoClientReply;

                    let mut new_ci = 0;
                    tokio::select! {
                        Some(request) = request_rx.recv() => {
                            let Ok(response) = PinnedClient::send_and_await_reply(&client, &"sequencer1".to_string(), request.as_ref()).await else {
                                continue;
                            };

                            let reply = ProtoClientReply::decode(&response.as_ref().0.as_slice()[0..response.as_ref().1]);
                            let Ok(reply) = reply else {
                                continue;
                            };
                            
                            new_ci = new_ci.max(reply.client_tag);
                            if new_ci > 1000 {
                                let _ = gc_tx.send((me.clone(), new_ci - 1000)).await;
                            }
                    
                            // Send the new commit index to the block broadcaster.
                            let _ = block_broadcaster_to_other_workers_tx.send(new_ci).await;
                    
                            // Send the commit index to the client reply handler.
                            let _ = client_reply_tx.send(new_ci);


                        }
                    }
                }
            });
        }
        staging.worker().await;
    }
    async fn worker(&mut self) {
        loop {
            tokio::select! {
                Some(vote) = self.vote_rx.recv() => {
                    self.preprocess_and_buffer_vote(vote).await;
                },
                Some(block) = self.block_rx.recv() => {
                    self.buffer_block(block).await;
                },
            }

            let new_ci = self.try_commit_blocks();

            if new_ci > self.commit_index {

                // Ordering here is important.
                // notify_downstream() needs to know the old commit index.
                // clean_up_buffer only works if the commit index is updated.
                self.notify_downstream(new_ci).await;
                self.commit_index = new_ci;
            }
            self.clean_up_buffer();
        }

    }

    async fn preprocess_and_buffer_vote(&mut self, vote: VoteWithSender) {
        let (sender, vote) = vote;
        self.vote_buffer
            .entry(vote.n).or_insert(Vec::new())
            .push((sender, vote));
    }

    async fn buffer_block(&mut self, block: CachedBlock) {
        self.block_buffer.push(block);
    }

    fn get_commit_threshold(&self) -> usize {

        let n = self.config.get().worker_config.storage_list.len() as usize;
        if n == 0 {
            return 0;
        }
        n / 2 + 1
    }

    fn try_commit_blocks(&mut self) -> u64 {
        let mut new_ci = self.commit_index;

        for block in &self.block_buffer {
            if block.block.n <= new_ci {
                continue;
            }

            let __blank = vec![];

            let votes = self.vote_buffer.get(&block.block.n).unwrap_or(&__blank);
            let blk_hsh = &block.block_hash;
            let vote_set = votes.iter()
                .filter(|(_, vote)| blk_hsh.eq(&vote.fork_digest))
                .map(|(sender, _)| sender.clone())
                .collect::<HashSet<_>>();


            if vote_set.len() >= self.get_commit_threshold() {
                new_ci = block.block.n;
            }
        }

        new_ci
    }

    fn clean_up_buffer(&mut self) {
        self.vote_buffer.retain(|n, _| *n > self.commit_index);
        self.block_buffer.retain(|block| block.block.n > self.commit_index);
    }

    async fn notify_downstream(&mut self, new_ci: u64) {
        // Send all blocks > self.commit_index <= new_ci to the logserver.
        let me = self.config.get().net_config.name.clone();
        let me = SenderType::Auth(me, self.chain_id);

        #[cfg(feature = "nimble")]
        let mut total_committed_blocks = 0;
        #[cfg(feature = "nimble")]
        let mut block_hash_buffer = Vec::new();

        for block in &self.block_buffer {
            if block.block.n > self.commit_index && block.block.n <= new_ci {
                #[cfg(feature = "nimble")]
                {
                    total_committed_blocks += 1;
                    block_hash_buffer.extend_from_slice(block.block_hash.as_ref());
                }

                let _ = self.logserver_tx.send((me.clone(), block.clone())).await;
            }
        }

        #[cfg(feature = "nimble")]
        if total_committed_blocks > 0 {
            use crate::crypto::hash;

            self.nimble_client_tag += 1;
            let hsh = hash(&block_hash_buffer);
            self.commit_to_nimble(hsh).await;
        }


        #[cfg(not(feature = "nimble"))]
        {
            if self.commit_index > 1000 {
                let _ = self.gc_tx.send((me.clone(), self.commit_index - 1000)).await;
            }
    
            // Send the new commit index to the block broadcaster.
            let _ = self.block_broadcaster_to_other_workers_tx.send(new_ci).await;
    
            // Send the commit index to the client reply handler.
            let res = self.client_reply_tx.send(new_ci);
            log::error!("Sent commit index to client reply handler: {:?}", res);
        }

    }


    #[cfg(feature = "nimble")]
    async fn commit_to_nimble(&self, block_hash: HashType) {

        use prost::Message as _;

        use crate::{proto::{client::ProtoClientRequest, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase}, rpc::ProtoPayload}, rpc::PinnedMessage};

        let client_request = ProtoClientRequest {
            tx: Some(ProtoTransaction {
                on_receive: Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: ProtoTransactionOpType::Write as i32,
                        operands: vec![block_hash],
                    }],
                }),
                on_crash_commit: None,
                on_byzantine_commit: None,
                is_reconfiguration: false,
                is_2pc: false,
            }),
            origin: self.config.get().net_config.name.clone(),
            sig: vec![0u8; 1],
            client_tag: self.nimble_client_tag,
        };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(client_request)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();

        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        // let _ = PinnedClient::send(&self.nimble_client, &"sequencer1".to_string(), request.as_ref()).await;

        // Probably be ok to have a bounded channel here.
        // I'm too tired to try anything right now.
        // This just means it takes some time longer to establish proper backpressure.
        let _ = self.nimble_request_sender_tx.send(request);
    }
}