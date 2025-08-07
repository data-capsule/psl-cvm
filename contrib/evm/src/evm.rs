use std::{pin::Pin, sync::Arc};

// Ported from https://github.com/bluealloy/revm/blob/main/examples/my_evm/src/evm.rs
use revm::{
    context::{ContextError, ContextSetters, ContextTr, Evm, FrameStack, Block, Transaction, Cfg, LocalContextTr},
    handler::{
        evm::FrameTr, instructions::EthInstructions, EthFrame, EthPrecompiles, EvmTr,
        FrameInitOrResult, ItemOrResult,
    },
    inspector::{InspectorEvmTr, JournalExt},
    interpreter::interpreter::EthInterpreter,
    Database, Inspector,
    database::{CacheDB, EmptyDBTyped, InMemoryDB, Journal},
    Context, MainContext,

};
use psl::utils::channel::{make_channel, Sender};
use psl::worker::app::{CacheConnector, ClientHandlerTask, UncommittedResultSet};
use psl::worker::TxWithAckChanTag;
use psl::worker::cache_manager::CacheCommand;
use tokio::{sync::{oneshot, Mutex}, task::JoinHandle};


#[derive(Debug)]
pub struct PslEvm<CTX, INSP> (
    pub Evm<
        CTX, INSP,
        EthInstructions<EthInterpreter, CTX>,
        EthPrecompiles,
        EthFrame<EthInterpreter>,
    >,
);

impl<CTX: ContextTr, INSP> PslEvm<CTX, INSP> {
    /// Creates a new instance of MyEvm with the provided context and inspector.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The execution context that manages state, environment, and journaling
    /// * `inspector` - The inspector for debugging and tracing execution
    ///
    /// # Returns
    ///
    /// A new MyEvm instance configured with:
    /// - The provided context and inspector
    /// - Mainnet instruction set
    /// - Default Ethereum precompiles
    /// - A fresh frame stack for execution
    pub fn new(ctx: CTX, inspector: INSP) -> Self {
        Self (
            Evm {
                ctx,
                inspector,
                instruction: EthInstructions::new_mainnet(),
                precompiles: EthPrecompiles::default(),
                frame_stack: FrameStack::new(),
            }
        )
    }
}

impl<CTX: ContextTr, INSP> EvmTr for PslEvm<CTX, INSP>
where
    CTX: ContextTr,
{
    type Context = CTX;
    type Instructions = EthInstructions<EthInterpreter, CTX>;
    type Precompiles = EthPrecompiles;
    type Frame = EthFrame<EthInterpreter>;
    fn ctx(&mut self) -> &mut Self::Context {
        &mut self.0.ctx
    }

    fn ctx_ref(&self) -> &Self::Context {
        self.0.ctx_ref()
    }

    fn ctx_instructions(&mut self) -> (&mut Self::Context, &mut Self::Instructions) {
        self.0.ctx_instructions()
    }

    fn ctx_precompiles(&mut self) -> (&mut Self::Context, &mut Self::Precompiles) {
        self.0.ctx_precompiles()
    }

    fn frame_stack(&mut self) -> &mut FrameStack<Self::Frame> {
        self.0.frame_stack()
    }

    fn frame_init(
        &mut self,
        frame_input: <Self::Frame as FrameTr>::FrameInit,
    ) -> Result<
        ItemOrResult<&mut Self::Frame, <Self::Frame as FrameTr>::FrameResult>,
        ContextError<<<Self::Context as ContextTr>::Db as Database>::Error>,
    > {
        self.0.frame_init(frame_input)
    }

    fn frame_run(
        &mut self,
    ) -> Result<
        FrameInitOrResult<Self::Frame>,
        ContextError<<<Self::Context as ContextTr>::Db as Database>::Error>,
    > {
        self.0.frame_run()
    }

    fn frame_return_result(
        &mut self,
        frame_result: <Self::Frame as FrameTr>::FrameResult,
    ) -> Result<
        Option<<Self::Frame as FrameTr>::FrameResult>,
        ContextError<<<Self::Context as ContextTr>::Db as Database>::Error>,
    > {
        self.0.frame_return_result(frame_result)
    }
}

impl<CTX: ContextTr, INSP> InspectorEvmTr for PslEvm<CTX, INSP>
where
    CTX: ContextSetters<Journal: JournalExt>,
    INSP: Inspector<CTX, EthInterpreter>,
{
    type Inspector = INSP;

    fn inspector(&mut self) -> &mut Self::Inspector {
        self.0.inspector()
    }

    fn ctx_inspector(&mut self) -> (&mut Self::Context, &mut Self::Inspector) {
        self.0.ctx_inspector()
    }

    fn ctx_inspector_frame(
        &mut self,
    ) -> (&mut Self::Context, &mut Self::Inspector, &mut Self::Frame) {
        self.0.ctx_inspector_frame()
    }

    fn ctx_inspector_frame_instructions(
        &mut self,
    ) -> (
        &mut Self::Context,
        &mut Self::Inspector,
        &mut Self::Frame,
        &mut Self::Instructions,
    ) {
        self.0.ctx_inspector_frame_instructions()
    }
}

pub struct PslEvmTask {
    evm_task_handle: JoinHandle<Result<(), anyhow::Error>>,
    evm_connector: Sender<oneshot::Receiver<Vec<u8>>>,
    cache_connector: CacheConnector,
    id: usize,
    total_work: usize,
}

impl ClientHandlerTask for PslEvmTask {
    fn new(cache_connector: Sender<CacheCommand>, id: usize) -> Self {
        let _cache_chan = cache_connector.clone();
        let (request_tx, request_rx) = make_channel(100);
        let handle = tokio::spawn(async move {
            let ctx = Context::mainnet().with_db(InMemoryDB::default());
            let evm = PslEvm::new(ctx, ());
            let cache_connector = CacheConnector::new(_cache_chan);
            while let Some(request) = request_rx.recv().await {
                // Run the evm
            }
            Ok(())
        });

        let cache_connector = CacheConnector::new(cache_connector);
        Self { evm_task_handle: handle, evm_connector: request_tx, cache_connector, id, total_work: 0 }
    }

    fn get_cache_connector(&self) -> &CacheConnector {
        &self.cache_connector
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn get_total_work(&self) -> usize {
        self.total_work
    }

    async fn on_client_request(&mut self, request: TxWithAckChanTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> Result<(), anyhow::Error> {
        // self.evm.run(request.tx).await;
        Ok(())
    }
}