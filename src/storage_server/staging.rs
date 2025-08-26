use std::{io::Error, pin::Pin, sync::Arc, u64};

use hashbrown::HashMap;
use log::{debug, error};
use prost::Message as _;
use tokio::sync::{mpsc::UnboundedSender, oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::{checkpoint::{ProtoAuthSenderType, ProtoBackfillQuery}, consensus::ProtoVote, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, MessageRef, SenderType}, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}};

use super::fork_receiver::ForkReceiverCommand;

pub struct Staging {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    client: PinnedClient,
    block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType /* sender */, String /* origin */)>, // Sender may not be equal to origin.
    logserver_tx: Sender<(SenderType, CachedBlock)>,
    gc_tx: Option<Sender<(SenderType, u64)>>,
    gc_timer: Arc<Pin<Box<ResettableTimer>>>,
    fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,

    last_confirmed_n: HashMap<SenderType, u64>,
    block_broadcaster_tx: Option<Sender<oneshot::Receiver<CachedBlock>>>,

    must_vote: bool, // Disabled in the sequencer.
}

const PER_PEER_BLOCK_WSS: u64 = 10000;

impl Staging {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        block_rx: Receiver<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType /* sender */, String /* origin */)>, // Sender may not be equal to origin.
        logserver_tx: Sender<(SenderType, CachedBlock)>,
        gc_tx: Option<Sender<(SenderType, u64)>>,
        fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
        block_broadcaster_tx: Option<Sender<oneshot::Receiver<CachedBlock>>>,
        must_vote: bool,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);
        let gc_timer = ResettableTimer::new(
            std::time::Duration::from_millis(config.get().app_config.checkpoint_interval_ms)
        );
        Self {
            config,
            keystore,
            block_rx,
            logserver_tx,
            fork_receiver_cmd_tx,
            gc_tx,
            client: client.into(),

            last_confirmed_n: HashMap::new(),
            gc_timer,
            block_broadcaster_tx,
            must_vote,
        }
    }

    pub async fn run(staging: Arc<Mutex<Staging>>) {
        let mut staging = staging.lock().await;

        staging.gc_timer.run().await;

        while let Ok(_) = staging.worker().await {
        
        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            _tick = self.gc_timer.wait() => {
                self.handle_gc().await?;
            },
            block_and_sender_and_origin = self.block_rx.recv() => {
                self.handle_block(block_and_sender_and_origin).await?;
            }
        }
        Ok(())
    }

    async fn handle_gc(&mut self) -> Result<(), ()> {
        if self.gc_tx.is_none() {
            return Ok(());
        }

        let gc_tx = self.gc_tx.as_ref().unwrap();

        for (sender, last_n) in self.last_confirmed_n.iter() {
            if *last_n > PER_PEER_BLOCK_WSS {
                let _ = gc_tx.send((sender.clone(), *last_n - PER_PEER_BLOCK_WSS)).await;
            }
        }
        Ok(())
    }



    async fn handle_block(&mut self, block_and_sender_and_origin: Option<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType, String)>) -> Result<(), ()> {
        if block_and_sender_and_origin.is_none() {
            return Err(());
        }

        let (block, sender, origin) = block_and_sender_and_origin.unwrap();

        let block = block.await;
        debug!("Received block {:?} from sender: {:?}", block, sender);

        if block.is_err() {
            return Err(());
        }

        let block = block.unwrap();

        match block {
            Ok(block) => {
                self.handle_checked_block(block, sender).await;
            }
            Err(err) => {
                // Handle error

                self.handle_error(err, sender, origin).await;
            }
        }


        Ok(())
    }

    /// 1. Confirm to fork receiver 
    /// 2. Send to logserver
    /// 3. Send vote to sender.
    async fn handle_checked_block(&mut self, block: CachedBlock, sender: SenderType) {
        let _ = self.fork_receiver_cmd_tx.send(
            ForkReceiverCommand::Confirm(sender.clone(), block.block.n)
        );

        let _ = self.logserver_tx.send((sender.clone(), block.clone())).await;

        let _ = match &self.block_broadcaster_tx {
            Some(tx) => {
                let (block_tx, block_rx) = oneshot::channel();
                let _ = tx.send(block_rx).await;
                block_tx.send(block.clone()).unwrap();
            }
            None => {
            }
        };

        let last_n = self.last_confirmed_n.entry(sender.clone())
            .or_insert(0);

        if block.block.n > *last_n {
            *last_n = block.block.n;
        }

        self.vote_on_block(block, sender).await;


    }

    /// 1. Rollback anything that is not confirmed.
    /// 2. Send backfill Nack to sender
    async fn handle_error(&mut self, err: Error, sender: SenderType, origin: String) {
        error!("Block verification error: {:?}", err);

        let last_n = self.last_confirmed_n.get(&sender).unwrap_or(&0);

        let _ = self.fork_receiver_cmd_tx.send(
            ForkReceiverCommand::Rollback(sender.clone(), *last_n)
        );

        let origin = ProtoAuthSenderType {
            name: origin,
            sub_id: 0,
        };

        self.nack(sender, 1 + *last_n, u64::MAX, origin).await;
    }


    async fn vote_on_block(&mut self, block: CachedBlock, sender: SenderType) {
        if !self.must_vote {
            return;
        }

        let (name, chain_id) = sender.to_name_and_sub_id();


        let vote = ProtoVote {
            fork_digest: block.block_hash.clone(),
            n: block.block.n,
            chain_id,
            // Unused
            sig_array: vec![],
            view: 0,
            config_num: 0,
        };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::Vote(vote)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();


        if name.contains("client"){
            debug!("Voting on test clients. Dropping vote."); // Useful for local testing
            return;
        }

        let _ = PinnedClient::send(&self.client, &name,
            MessageRef(&buf, sz, &SenderType::Anon)
        ).await;


    }

    async fn nack(&mut self, sender: SenderType, start_index: u64, end_index: u64, origin: ProtoAuthSenderType) {
        let my_name = self.config.get().net_config.name.clone();
        let nack = ProtoBackfillQuery {
            start_index,
            end_index,
            reply_name: my_name,
            origin: Some(origin),
        };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::BackfillQuery(nack)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();

        let (name, _) = sender.to_name_and_sub_id();

        let _ = PinnedClient::send(&self.client, &name,
            MessageRef(&buf, sz, &SenderType::Anon)
        ).await;
    }

}