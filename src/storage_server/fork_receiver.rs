use std::{collections::{HashMap, VecDeque}, io::Error, sync::Arc};

use tokio::sync::{mpsc::UnboundedReceiver, oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock, CryptoServiceConnector, FutureHash, HashType}, proto::consensus::{HalfSerializedBlock, ProtoAppendEntries}, rpc::{client::{Client, PinnedClient}, SenderType}, utils::{channel::{Receiver, Sender}, StorageServiceConnector}};

pub struct ContinuityStats {
    pub block_n: u64,
    pub block_hash: FutureHash,
}

pub enum ForkReceiverCommand {
    Confirm(SenderType, u64),
    Rollback(SenderType, u64),
}

pub struct ForkReceiver {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    client: PinnedClient,

    ae_rx: Receiver<(ProtoAppendEntries, SenderType)>,
    crypto: CryptoServiceConnector,
    storage: StorageServiceConnector,
    staging_tx: Sender<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType)>,

    /// This is back-channel from staging, hence unbounded.
    /// Otherwise, it may cause a deadlock.
    cmd_rx: UnboundedReceiver<ForkReceiverCommand>,

    /// Invariant: All continuity stats are in order &&
    /// all continuity stats (except maybe the first one) is unconfirmed. 
    continuity_stats: HashMap<SenderType, VecDeque<ContinuityStats>>,

}

impl ForkReceiver {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        ae_rx: Receiver<(ProtoAppendEntries, SenderType)>,
        crypto: CryptoServiceConnector,
        storage: StorageServiceConnector,
        staging_tx: Sender<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType)>,
        cmd_rx: UnboundedReceiver<ForkReceiverCommand>,
    ) -> Self {

        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);

        Self {
            config,
            keystore,
            ae_rx,
            crypto,
            storage,
            staging_tx,
            cmd_rx,
            client: client.into(),

            continuity_stats: HashMap::new(),
        }
    }

    pub async fn run(fork_receiver: Arc<Mutex<ForkReceiver>>) {
        let mut fork_receiver = fork_receiver.lock().await;

        while let Ok(_) = fork_receiver.worker().await {

        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        let pending_cmds = self.cmd_rx.len();
        if pending_cmds > 0 {
            // Prefer clearing cmd_rx as fast as possible.
            let mut cmds = Vec::new();
            self.cmd_rx.recv_many(&mut cmds, pending_cmds);

            for cmd in cmds {
                self.handle_command(cmd).await?;
            }

            return Ok(())
        }

        // Otherwise, wait for all.

        tokio::select! {
            Some((ae, sender)) = self.ae_rx.recv() => {
                // Handle AppendEntries
                self.handle_ae(ae, sender).await?;
            }
            Some(cmd) = self.cmd_rx.recv() => {
                // Handle commands
                self.handle_command(cmd).await?;
            }
        }

        Ok(())
    }


    async fn handle_command(&mut self, cmd: ForkReceiverCommand) -> Result<(), ()> {
        match cmd {
            // Everything <= n is confirmed.
            // Clear it from stats.
            // For continuity checking, if stats is emptied, keep the last entry, even if it is confirmed.
            ForkReceiverCommand::Confirm(sender, n) => {
                if !self.continuity_stats.contains_key(&sender) {
                    return Ok(());
                }

                let stats = self.continuity_stats.get_mut(&sender).unwrap();

                if stats.len() == 0 {
                    return Ok(());
                }

                let last_entry = stats.back().unwrap().clone();
                
                
                let to_remove_n = if last_entry.block_n <= n {
                    last_entry.block_n - 1
                } else {
                    n
                };

                stats.retain(|b| b.block_n > to_remove_n);

            },

            // Everything >= n must be rolled back.
            // Maybe the blocks weren't valid.
            ForkReceiverCommand::Rollback(sender, n) => {
                if !self.continuity_stats.contains_key(&sender) {
                    return Ok(());
                }

                let stats = self.continuity_stats.get_mut(&sender).unwrap();

                stats.retain(|b| b.block_n < n);
            }
        }
        Ok(())
    }

    async fn handle_ae(&mut self, ae: ProtoAppendEntries, sender: SenderType) -> Result<(), ()> {
        if ae.fork.is_none() {
            return Ok(());
        }

        let mut fork = ae.fork.unwrap();
        if fork.serialized_blocks.len() == 0 {
            return Ok(());
        }
        
        let stats = self.continuity_stats
            .entry(sender.clone())
            .or_insert(VecDeque::new());

        
        for block in fork.serialized_blocks.drain(..) {
            let _n = block.n;

            // TODO: This step can be made constant time!
            // This is currently O(# pending blocks).
            let parent_hash = Self::find_parent_hash(stats, &block).await;
            let (fut_block, hash, _parent_hash) = self.crypto.verify_and_prepare_block_simple(block.serialized_body, parent_hash, sender.clone()).await;

            Self::append_block(stats, _n, hash).await;
            Self::reset_parent_hash(stats, _n - 1, _parent_hash).await;

            // Forward it to storage.
            let storage_acked_block = self.storage.put_nonblocking(fut_block).await;

            let _ = self.staging_tx.send((storage_acked_block, sender.clone())).await;
        }

        Ok(())
    }

    async fn find_parent_hash(stats: &mut VecDeque<ContinuityStats>, block: &HalfSerializedBlock) -> FutureHash {
        let search_n = block.n - 1;
        stats.binary_search_by_key(&search_n, |b| b.block_n).map(|i| {
            stats[i].block_hash.take()
        }).unwrap_or_else(|_| {
            // Not found, return zero hash.
            // This is the first block in the chain.
            FutureHash::None
        })
    }

    async fn append_block(stats: &mut VecDeque<ContinuityStats>, n: u64, hash: FutureHash) {
        stats.push_back(ContinuityStats {
            block_n: n,
            block_hash: hash,
        });
    }

    async fn reset_parent_hash(stats: &mut VecDeque<ContinuityStats>, n: u64, parent_hash: FutureHash) {
        stats.binary_search_by_key(&n, |b| b.block_n).map(|i| {
            stats[i].block_hash = parent_hash;
        }).unwrap_or_else(|_| {
            // Not found, do nothing.
        });
    }
}