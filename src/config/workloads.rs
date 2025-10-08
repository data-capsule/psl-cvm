use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Copy)]
pub struct KVReadWriteUniform {
    pub num_keys: usize,
    pub val_size: usize,
    pub read_ratio: f64,
    pub write_byz_commit_ratio: f64
}


/// We are only going to support YCSB-A, B and C.
/// These don't need the Latest distribution and are only updates/reads, so distributions need not be re-calculated.
#[derive(Serialize, Deserialize, Clone, Debug, Copy)]
pub struct KVReadWriteYCSB {
    pub read_ratio: f64,
    pub linearizable_reads: bool,       // Reads go through consensus.
    pub byz_commit_ratio: f64,
    pub val_size: usize,
    pub num_fields: usize,
    pub num_keys: usize,
    pub zipf_exponent: f64,
    pub load_phase: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Copy)]
pub struct Blanks {

    #[serde(default = "default_payload_size")]
    pub payload_size: usize,

    #[serde(default = "default_signature_interval")]
    pub signature_interval: usize,
}

const fn default_payload_size() -> usize {
    512
}

const fn default_signature_interval() -> usize {
    usize::MAX
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Smallbank {
    pub frequency_amalgamate: u64,
    pub frequency_write_check: u64,
    pub frequency_deposit_checking: u64,
    pub frequency_transact_savings: u64,
    pub frequency_send_payment: u64,
    pub frequency_balance: u64,

    pub num_accounts: usize,
    pub min_balance: f64,
    pub max_balance: f64,

    pub load_phase: bool,
}

impl Smallbank {
    pub fn new(frequency_amalgamate: u64, frequency_write_check: u64, frequency_deposit_checking: u64, frequency_transact_savings: u64, frequency_send_payment: u64, frequency_balance: u64, num_accounts: usize, min_balance: f64, max_balance: f64, load_phase: bool) -> Self {
        assert!(frequency_amalgamate + frequency_write_check + frequency_deposit_checking + frequency_transact_savings + frequency_send_payment + frequency_balance == 100);
        
        
        Self { frequency_amalgamate, frequency_write_check, frequency_deposit_checking, frequency_transact_savings, frequency_send_payment, frequency_balance, num_accounts, min_balance, max_balance, load_phase }
    }
}


impl Default for Smallbank {
    /// Taken from: https://github.com/apavlo/h-store/blob/master/src/benchmarks/edu/brown/benchmark/smallbank/SmallBankConstants.java
    fn default() -> Self {
        let frequency_amalgamate = 15;
        let frequency_write_check = 15;
        let frequency_deposit_checking = 15;
        let frequency_transact_savings = 15;
        let frequency_send_payment = 25;
        let frequency_balance = 15;
        let num_accounts = 1_000_000;

        // This is done so that accounts never run out of money.
        let min_balance = 10_000.0;
        let max_balance = 50_000.0;

        let load_phase = true;
        Self::new(frequency_amalgamate, frequency_write_check, frequency_deposit_checking, frequency_transact_savings, frequency_send_payment, frequency_balance, num_accounts, min_balance, max_balance, load_phase)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RequestConfig {
    Blanks,
    AEBlanks(Blanks),
    KVReadWriteUniform(KVReadWriteUniform),
    KVReadWriteYCSB(KVReadWriteYCSB),
    MockSQL(),
    MLTraining(String),
    Smallbank(#[serde(default)] Smallbank),
}
