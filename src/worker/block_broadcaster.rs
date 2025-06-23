use std::sync::Arc;

use lz4_flex::block;
use prost::Message;
use tokio::{sync::Mutex, sync::oneshot};

use crate::{config::AtomicConfig, crypto::{CachedBlock, CryptoServiceConnector}, proto::consensus::{HalfSerializedBlock, ProtoAppendEntries, ProtoFork}, rpc::{client::PinnedClient, server::LatencyProfile, MessageRef, PinnedMessage, SenderType}, utils::channel::{Receiver, Sender}};


pub enum BroadcastMode {
    Star(String /* Name prefix to send to */),
    Gossip(Vec<String> /* Names to send to */),
    // RandomGossip(String) // TODO: Implement this
}



pub struct BlockBroadcaster {
    config: AtomicConfig,
    crypto: CryptoServiceConnector, // Need it to encrypt the block while sending to storage.
    client: PinnedClient,

    broadcast_mode: BroadcastMode,
    forward_to_staging: bool,

    block_rx: Receiver<oneshot::Receiver<CachedBlock>>,
    staging_tx: Sender<CachedBlock>,

}

impl BlockBroadcaster {
    pub fn new(config: AtomicConfig, client: PinnedClient, crypto: CryptoServiceConnector, broadcast_mode: BroadcastMode, forward_to_staging: bool, block_rx: Receiver<oneshot::Receiver<CachedBlock>>, staging_tx: Sender<CachedBlock>) -> Self {
        Self {
            config,
            crypto,
            client,
            broadcast_mode,
            forward_to_staging,
            block_rx,
            staging_tx,
        }
    }

    fn get_peers(&self) -> Vec<String> {
        match &self.broadcast_mode {
            BroadcastMode::Star(name_prefix) => {
                self.config.get().consensus_config.node_list
                .iter().filter(|name| name.starts_with(name_prefix))
                .map(|name| name.to_string())
                .collect()
            }
            BroadcastMode::Gossip(names) => names.clone(),
        }
    }

    fn get_success_threshold(&self) -> usize {
        match &self.broadcast_mode {
            BroadcastMode::Star(_) => self.config.get().consensus_config.node_list.len() / 2 + 1,
            BroadcastMode::Gossip(_) => 0,
        }
    }

    fn wrap_block_for_broadcast(&self, block: &CachedBlock) -> ProtoAppendEntries {
        ProtoAppendEntries {
            view: block.block.view,
            config_num: block.block.config_num,
            fork: Some(ProtoFork {
                serialized_blocks: vec![HalfSerializedBlock {
                    n: block.block.n,
                    serialized_body: block.block_ser.clone(),

                    // Unused fields
                    view: block.block.view,
                    view_is_stable: block.block.view_is_stable,
                    config_num: block.block.config_num,
                }]
            }),

            // Unused fields
            commit_index: 0,
            view_is_stable: true,
            is_backfill_response: false,
        }
    }

    pub async fn run(block_broadcaster: Arc<Mutex<BlockBroadcaster>>) {
        let mut block_broadcaster = block_broadcaster.lock().await;
        block_broadcaster.worker().await;
    }

    async fn worker(&mut self) {
        while let Some(block_rx) = self.block_rx.recv().await {
            let block = block_rx.await.unwrap();

            let peers = self.get_peers();
            let threshold = self.get_success_threshold();

            let ae = self.wrap_block_for_broadcast(&block);
            let data = ae.encode_to_vec();

            let sz = data.len();
            let data = PinnedMessage::from(data, sz, SenderType::Anon);

            let _ = PinnedClient::broadcast(
                &self.client,
                &peers, &data, 
                &mut LatencyProfile::new(),
                threshold
            ).await;

            if self.forward_to_staging {
                let _ = self.staging_tx.send(block).await;
            }
            
        }
    }
}