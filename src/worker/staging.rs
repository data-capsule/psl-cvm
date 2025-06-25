use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
use log::warn;
use tokio::sync::Mutex;

use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, crypto::{CachedBlock, CryptoServiceConnector}, proto::consensus::ProtoVote, rpc::SenderType, utils::channel::{Receiver, Sender}};

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

}

impl Staging {
    pub fn new(config: AtomicPSLWorkerConfig, crypto: CryptoServiceConnector, vote_rx: Receiver<VoteWithSender>, block_rx: Receiver<CachedBlock>, block_broadcaster_to_other_workers_tx: Sender<u64>, logserver_tx: Sender<(SenderType, CachedBlock)>, client_reply_tx: tokio::sync::broadcast::Sender<u64>, gc_tx: Sender<(SenderType, u64)>) -> Self {
        Self {
            config,
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
        }
    }

    pub async fn run(staging: Arc<Mutex<Self>>) {
        let mut staging = staging.lock().await;
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

        // TODO: This is not correct. Change the config to reflect the quorum size.
        let n = self.config.get().worker_config.storage_list.len() as usize;
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
        let me = SenderType::Auth(me, 0);
        for block in &self.block_buffer {
            if block.block.n > self.commit_index && block.block.n <= new_ci {
                let _ = self.logserver_tx.send((me.clone(), block.clone())).await;
            }
        }

        if self.commit_index > 1000 {
            let _ = self.gc_tx.send((me.clone(), self.commit_index - 1000)).await;
        }

        // Send the new commit index to the block broadcaster.
        let _ = self.block_broadcaster_to_other_workers_tx.send(new_ci).await;

        // Send the commit index to the client reply handler.
        let _ = self.client_reply_tx.send(new_ci);
    }
}