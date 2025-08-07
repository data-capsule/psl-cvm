// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use log::{debug, error, info};
use psl::config::{self, Config, PSLWorkerConfig};
use psl::utils::channel::{make_channel, Receiver, Sender};
use psl::worker::TxWithAckChanTag;
use psl::{consensus, storage_server, worker};
use rand::{thread_rng, RngCore};
use revm::context::ContextTr;
use revm::database::async_db::DatabaseAsyncRef;
use revm::database::WrapDatabaseAsync;
use revm::handler::EvmTr;
use revm::primitives::ruint::Uint;
use revm::primitives::{address, hex, Address, TxKind};
use revm::state::{Account, AccountInfo};
use revm::{context::TxEnv,
    handler::{ExecuteCommitEvm, ExecuteEvm, Handler}, Context, MainContext};
use tokio::time::sleep;
use tokio::{runtime, signal};
use std::process::exit;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use std::{env, fs, io, path, sync::{atomic::AtomicUsize, Arc, Mutex}};
use psl::consensus::engines::kvs::KVSAppEngine;
use std::io::{Bytes, Write};
use serde::{Deserialize, Serialize};

mod evm;
mod db;


use crate::db::PslKvDb;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;


/// Fetch json config file from command line path.
/// Panic if not found or parsed properly.
fn process_args() -> (PSLWorkerConfig, String) {
    macro_rules! usage_str {
        () => {
            "\x1b[31;1mUsage: {} path/to/config.json path/to/smart_contract.json\x1b[0m"
        };
    }

    let args: Vec<_> = env::args().collect();

    if args.len() != 3 {
        panic!(usage_str!(), args[0]);
    }
    
    let cfg_path = path::Path::new(args[1].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }
    
    let cfg_contents = fs::read_to_string(cfg_path).expect("Invalid file path");
    
    let cfg = PSLWorkerConfig::deserialize(&cfg_contents);
    let smart_contract_path = args[2].clone();

    (cfg, smart_contract_path)
}

#[allow(unused_assignments)]
fn get_feature_set() -> (&'static str, &'static str) {
    let mut app = "";
    let mut protocol = "";

    #[cfg(feature = "app_logger")]{ app = "app_logger"; }
    #[cfg(feature = "app_kvs")]{ app = "app_kvs"; }
    #[cfg(feature = "app_sql")]{ app = "app_sql"; }

    #[cfg(feature = "lucky_raft")]{ protocol = "lucky_raft"; }
    #[cfg(feature = "signed_raft")]{ protocol = "signed_raft"; }
    #[cfg(feature = "chained_pbft")]{ protocol = "chained_pbft"; }
    #[cfg(feature = "pirateship")]{ protocol = "pirateship"; }
    #[cfg(feature = "engraft")]{ protocol = "engraft"; }

    (protocol, app)
}

enum NodeType {
    Worker(worker::PSLWorker<worker::app::KVSTask>),
}

async fn run_worker(cfg: PSLWorkerConfig) -> NodeType {
    let node = worker::PSLWorker::<worker::app::KVSTask>::new(cfg);
    NodeType::Worker(node)
}

macro_rules! handle_signal_till_end {
    ($node:expr) => {
        let mut handles = $node.run().await;
        match signal::ctrl_c().await {
            Ok(_) => {
                info!("Received SIGINT. Shutting down.");
                handles.abort_all();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                info!("Force shutdown.");
                exit(0);
            },
            Err(e) => {
                error!("Signal: {:?}", e);
            }
        }
    
        while let Some(res) = handles.join_next().await {
            info!("Task completed with {:?}", res);
        }

    };
}

async fn run_main(cfg: PSLWorkerConfig, client_request_tx: Sender<TxWithAckChanTag>, client_request_rx: Receiver<TxWithAckChanTag>) -> Result<(), io::Error> {
    let mut node = worker::PSLWorker::<worker::app::KVSTask>::mew(cfg, client_request_tx, client_request_rx);
    handle_signal_till_end!(node);

    Ok(())
}


const NUM_THREADS: usize = 32;

fn main() {
    log4rs::init_config(config::default_log4rs_config()).unwrap();

    let (config, smart_contract_path) = process_args();

    let (protocol, app) = get_feature_set();
    info!("Protocol: {}, App: {}", protocol, app);

    #[cfg(feature = "evil")]
    if cfg.evil_config.simulate_byzantine_behavior {
        warn!("Will simulate Byzantine behavior!");
    }

    let core_ids = 
        Arc::new(Mutex::new(Box::pin(core_affinity::get_core_ids().unwrap())));

    let start_idx = 0; // config.worker_config.all_worker_list.iter().position(|r| r.eq(&config.net_config.name)).unwrap();
    let mut num_threads = NUM_THREADS;
    {
        let _num_cores = core_ids.lock().unwrap().len();
        if _num_cores - 1 < num_threads {
            // Leave one core for the storage compaction thread.
            num_threads = _num_cores - 1;
        }
    }

    let start_idx = 0; //start_idx * num_threads;

    let (client_request_tx, client_request_rx) = make_channel(1000);
    
    let i = Box::pin(AtomicUsize::new(0));
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_threads / 2)
        .on_thread_start(move || {
            // let _cids = core_ids.clone();
            // let lcores = _cids.lock().unwrap();
            // let id = (start_idx + i.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) % lcores.len();
            // let res = core_affinity::set_for_current(lcores[id]);
    
            // if res {
            //     debug!("Thread pinned to core {:?}", id);
            // }else{
            //     debug!("Thread pinning to core {:?} failed", id);
            // }
            // std::io::stdout().flush()
            //     .unwrap();
        })
        .build()
        .unwrap();



    let state = PslKvDb {
        db_chan: Some(client_request_tx.clone()),
        client_tag: AtomicU64::new(1),
    };

    runtime.spawn(run_main(config, client_request_tx, client_request_rx));
    let _ = runtime.block_on(run_evm(state, smart_contract_path));

}


async fn run_evm(mut state: PslKvDb, smart_contract_path: String) {
    sleep(Duration::from_secs(2)).await;
    state.insert_account_info(address!("0xcafebabecafebabecafebabecafebabecafebabe"), AccountInfo::from_balance(Uint::<256, 4>::from(1000000000000000000000000u128))).await;
    state.insert_account_info(address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"), AccountInfo::from_balance(Uint::<256, 4>::from(0u64))).await;


    {
        let account1 = state.load_account(address!("0xcafebabecafebabecafebabecafebabecafebabe")).await.unwrap();
        info!("Account 1: {:?}", account1.info.balance);
    }

    let mut my_evm = evm::PslEvm::new(Context::mainnet().with_db(state), ());
    
    let tx = TxEnv::builder()
        .caller(address!("0xcafebabecafebabecafebabecafebabecafebabe"))
        .to(address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"))
        .value(Uint::<256, 4>::from(1000000000000000000u64))
        .build().unwrap();
    // let tx = TxEnv::default();

    let _res = my_evm.transact_commit(tx);
    info!("EVM result: {:?}", _res);

    
    let build_json = fs::read_to_string(smart_contract_path).unwrap();
    let build_json: serde_json::Value = serde_json::from_str(&build_json).unwrap();

    let contract_list = build_json["contracts"].as_object().unwrap();

    let mut nonce = 0;

    for (name, val) in contract_list {
        // info!("Contract: {:?} {}", name, val["evm"]["deployedBytecode"]["object"].to_string());
        info!("Deploying contract: {:?}", name);

        let bytecode = val["evm"]["deployedBytecode"]["object"].as_str().unwrap().to_string();
        let bytecode = hex::decode(bytecode).unwrap();
        let bytecode = revm::primitives::Bytes::from(bytecode);

        let contract_tx = TxEnv::builder()
            .kind(TxKind::Create)
            .data(bytecode)
            .nonce(nonce)
            .build()
            .unwrap();
        nonce += 1;
    
        let _res = my_evm.transact_commit(contract_tx);
        info!("EVM result: {:?}", _res);
    }

    let ctx = my_evm.ctx();
    let state = ctx.db();

    let account2 = state.load_account(address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")).await.unwrap();
    info!("Account 2: {:?}", account2.info.balance);

    let account1 = state.load_account(address!("0xcafebabecafebabecafebabecafebabecafebabe")).await.unwrap();
    info!("Account 1: {:?}", account1.info.balance);



    sleep(Duration::from_secs(2)).await;
}