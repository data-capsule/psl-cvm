use std::{pin::Pin, sync::Arc};

// Ported from https://github.com/bluealloy/revm/blob/main/examples/my_evm/src/evm.rs
use revm::{
    context::{result::{EVMError, ExecResultAndState, ExecutionResult, HaltReason, InvalidTransaction}, Block, Cfg, ContextError, ContextSetters, ContextTr, Evm, FrameStack, JournalTr, LocalContextTr, Transaction}, database::{CacheDB, EmptyDBTyped, InMemoryDB}, handler::{
        evm::FrameTr, instructions::{EthInstructions, InstructionProvider}, EthFrame, EthPrecompiles, EvmTr, FrameInitOrResult, FrameResult, Handler, ItemOrResult, PrecompileProvider
    }, inspector::{InspectorEvmTr, InspectorHandler, JournalExt}, interpreter::{interpreter::EthInterpreter, interpreter_action::FrameInit, InterpreterResult}, state::EvmState, Context, Database, DatabaseCommit, ExecuteCommitEvm, ExecuteEvm, InspectCommitEvm, InspectEvm, Inspector, MainContext

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


/// Type alias for the error type of the OpEvm.
type PslEvmError<CTX> = EVMError<<<CTX as ContextTr>::Db as Database>::Error, InvalidTransaction>;

// Trait that allows to replay and transact the transaction.
impl<CTX, INSP> ExecuteEvm for PslEvm<CTX, INSP>
where
    CTX: ContextSetters<Journal: JournalTr<State = EvmState>>,
{
    type State = EvmState;
    type ExecutionResult = ExecutionResult<HaltReason>;
    type Error = PslEvmError<CTX>;

    type Tx = <CTX as ContextTr>::Tx;

    type Block = <CTX as ContextTr>::Block;

    fn set_block(&mut self, block: Self::Block) {
        self.0.ctx.set_block(block);
    }

    fn transact_one(&mut self, tx: Self::Tx) -> Result<Self::ExecutionResult, Self::Error> {
        self.0.ctx.set_tx(tx);
        let mut handler = PslHandler::default();
        handler.run(self)
    }

    fn finalize(&mut self) -> Self::State {
        self.ctx().journal_mut().finalize()
    }

    fn replay(
        &mut self,
    ) -> Result<ExecResultAndState<Self::ExecutionResult, Self::State>, Self::Error> {
        let mut handler = PslHandler::default();
        handler.run(self).map(|result| {
            let state = self.finalize();
            ExecResultAndState::new(result, state)
        })
    }
}

// Trait allows replay_commit and transact_commit functionality.
impl<CTX, INSP> ExecuteCommitEvm for PslEvm<CTX, INSP>
where
    CTX: ContextSetters<Db: DatabaseCommit, Journal: JournalTr<State = EvmState>>,
{
    fn commit(&mut self, state: Self::State) {
        self.ctx().db_mut().commit(state);
    }
}

// Inspection trait.
impl<CTX, INSP> InspectEvm for PslEvm<CTX, INSP>
where
    CTX: ContextSetters<Journal: JournalTr<State = EvmState> + JournalExt>,
    INSP: Inspector<CTX, EthInterpreter>,
{
    type Inspector = INSP;

    fn set_inspector(&mut self, inspector: Self::Inspector) {
        self.0.inspector = inspector;
    }

    fn inspect_one_tx(&mut self, tx: Self::Tx) -> Result<Self::ExecutionResult, Self::Error> {
        self.0.ctx.set_tx(tx);
        let mut handler = PslHandler::default();
        handler.inspect_run(self)
    }
}

// Inspect
impl<CTX, INSP> InspectCommitEvm for PslEvm<CTX, INSP>
where
    CTX: ContextSetters<Db: DatabaseCommit, Journal: JournalTr<State = EvmState> + JournalExt>,
    INSP: Inspector<CTX, EthInterpreter>,
{
}

/// Custom handler for MyEvm that defines transaction execution behavior.
///
/// This handler demonstrates how to customize EVM execution by implementing
/// the Handler trait. It can be extended to add custom validation, modify
/// gas calculations, or implement protocol-specific behavior while maintaining
/// compatibility with the standard EVM execution flow.
#[derive(Debug)]
pub struct PslHandler<EVM> {
    /// Phantom data to maintain the EVM type parameter.
    /// This field exists solely to satisfy Rust's type system requirements
    /// for generic parameters that aren't directly used in the struct fields.
    pub _phantom: core::marker::PhantomData<EVM>,
}

impl<EVM> Default for PslHandler<EVM> {
    fn default() -> Self {
        Self {
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<EVM> Handler for PslHandler<EVM>
where
    EVM: EvmTr<
        Context: ContextTr<Journal: JournalTr<State = EvmState>>,
        Precompiles: PrecompileProvider<EVM::Context, Output = InterpreterResult>,
        Instructions: InstructionProvider<
            Context = EVM::Context,
            InterpreterTypes = EthInterpreter,
        >,
        Frame: FrameTr<FrameResult = FrameResult, FrameInit = FrameInit>,
    >,
{
    type Evm = EVM;
    type Error = EVMError<<<EVM::Context as ContextTr>::Db as Database>::Error, InvalidTransaction>;
    type HaltReason = HaltReason;

    fn reward_beneficiary(
        &self,
        _evm: &mut Self::Evm,
        _exec_result: &mut FrameResult,
    ) -> Result<(), Self::Error> {
        // Skip beneficiary reward
        Ok(())
    }
}

impl<EVM> InspectorHandler for PslHandler<EVM>
where
    EVM: InspectorEvmTr<
        Inspector: Inspector<<<Self as Handler>::Evm as EvmTr>::Context, EthInterpreter>,
        Context: ContextTr<Journal: JournalTr<State = EvmState>>,
        Precompiles: PrecompileProvider<EVM::Context, Output = InterpreterResult>,
        Instructions: InstructionProvider<
            Context = EVM::Context,
            InterpreterTypes = EthInterpreter,
        >,
    >,
{
    type IT = EthInterpreter;
}