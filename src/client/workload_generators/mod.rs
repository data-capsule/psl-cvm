use crate::proto::execution::{ProtoTransaction, ProtoTransactionResult};


/// Who do I send the request to?
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Executor {
    Leader = 1,
    Any = 2
}


/// What's the carrier struct?
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum WrapperMode {
    ClientRequest = 1,
    AppendEntries = 2,
    AppendEntriesWithSignature = 3,
}

/// How to pace requests?
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum RateControl {
    CloseLoop = 1,
    OpenLoop = 2,
}


pub struct WorkloadUnit {
    pub tx: ProtoTransaction,
    pub executor: Executor,
    pub wrapper_mode: WrapperMode,
    pub rate_control: RateControl,
}

pub trait PerWorkerWorkloadGenerator: Send {
    fn next(&mut self) -> WorkloadUnit;
    fn check_result(&mut self, result: &Option<ProtoTransactionResult>) -> bool;
}

mod blanks;
pub use blanks::*;

mod ae_blanks;
pub use ae_blanks::*;

mod kv_uniform;
pub use kv_uniform::*;

mod kv_ycsb;
pub use kv_ycsb::*;

mod mocksql;
pub use mocksql::*;

mod ml_training;
pub use ml_training::*;
mod smallbank;
pub use smallbank::*;
