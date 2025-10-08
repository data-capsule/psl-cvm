use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use log::info;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{CachedBlock, HashType}, rpc::SenderType, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::block_sequencer::VectorClock};

pub struct CommitBuffer {
    config: AtomicConfig,

    block_rx: Receiver<(SenderType, CachedBlock)>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    uncommitted_buffer: HashMap<(String /* origin */, u64 /* seq_num */), (BlockStats, HashType, CachedBlock, usize /* votes */)>,
    commit_indices: HashMap<String /* origin */, u64 /* seq_num */>,

    auditor_tx: tokio::sync::mpsc::UnboundedSender<CachedBlock>,
}

#[derive(Clone)]
pub struct BlockStats {
    pub(crate) origin: String,
    pub(crate) read_vc: VectorClock,
    pub(crate) seq_num: u64,

}

impl CommitBuffer {
    pub fn new(config: AtomicConfig, block_rx: Receiver<(SenderType, CachedBlock)>, auditor_tx: tokio::sync::mpsc::UnboundedSender<CachedBlock>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config, block_rx, log_timer,
            uncommitted_buffer: HashMap::new(),
            commit_indices: HashMap::new(),
            auditor_tx,
        }
    }

    pub async fn run(controller:Arc<Mutex<Self>>) {
        let mut controller = controller.lock().await;
        controller.log_timer.run().await;
        

        while let Ok(()) = controller.worker().await {

        }
    }

    async fn worker(&mut self) -> Result<(), anyhow::Error> {
        tokio::select! {
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            }
            Some((_sender, block)) = self.block_rx.recv() => {
                self.handle_block(block, _sender).await;
            }
        }

        Ok(())
    }


    async fn handle_block(&mut self, block: CachedBlock, _: SenderType) {
        // The sender is going to be unused.
        let origin = block.block.origin.clone();

        let read_vc = VectorClock::from(block.block.vector_clock.clone());

        let seq_num = block.block.n;

        let block_stats = BlockStats {
            origin: origin.clone(),
            read_vc,
            seq_num,
        };

        if let Some(n) = self.commit_indices.get(&origin) {
            if *n >= seq_num {
                // Just drop the block.
                return;
            }
        }

        let commit_threshold = self.get_commit_threshold();
        
        
        let entry = self.uncommitted_buffer
            .entry((origin.clone(), seq_num))
            .or_insert((block_stats, block.block_hash.clone(), block.clone(), 0));

        if entry.1 != block.block_hash {
            self.throw_error().await;
            return;
        }

        entry.3 += 1;

        if entry.3 >= commit_threshold {
            self.commit_indices.insert(origin.clone(), seq_num);
            let val = self.uncommitted_buffer.remove(&(origin.clone(), seq_num)).unwrap();
            self.auditor_tx.send(val.2).unwrap();
        }

    }

    fn get_commit_threshold(&self) -> usize {
        let n = self.config.get().consensus_config.node_list.len();
        if n == 0 {
            return 0;
        }

        n / 2 + 1
    }

    async fn log_stats(&mut self) {
        info!("Uncommitted buffer size: {}", self.uncommitted_buffer.len());
        info!("Commit indices: {:?}", self.commit_indices);
    }

    async fn throw_error(&mut self) {
        unimplemented!()
    }
}