// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use log::{debug, error, info};
use psl::config::{self, Config, PSLWorkerConfig};
use psl::{sequencer, storage_server, worker};
use tokio::{runtime, signal};
use std::process::exit;
use std::{env, fs, io, path, sync::{atomic::AtomicUsize, Arc, Mutex}};
use std::io::Write;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

enum RunMode {
    Storage(Config),
    Worker(PSLWorkerConfig),
    Sequencer(Config),
}

/// Fetch json config file from command line path.
/// Panic if not found or parsed properly.
fn process_args() -> RunMode {
    macro_rules! usage_str {
        () => {
            "\x1b[31;1mUsage: {} storage|worker|sequencer path/to/config.json\x1b[0m"
        };
    }

    let args: Vec<_> = env::args().collect();

    if args.len() != 3 {
        panic!(usage_str!(), args[0]);
    }

    if args[1] != "storage" && args[1] != "worker" && args[1] != "sequencer" {
        panic!(usage_str!(), args[0]);
    }

    
    let cfg_path = path::Path::new(args[2].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }
    
    let cfg_contents = fs::read_to_string(cfg_path).expect("Invalid file path");
    
    let run_mode = match args[1].as_str() {
        "storage" => RunMode::Storage(Config::deserialize(&cfg_contents)),
        "worker" => RunMode::Worker(PSLWorkerConfig::deserialize(&cfg_contents)),
        "sequencer" => RunMode::Sequencer(Config::deserialize(&cfg_contents)),
        _ => panic!(usage_str!(), args[0]),
    };

    run_mode
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
    Sequencer(sequencer::SequencerNode),

    Worker(worker::PSLWorker<worker::engines::AbortableKVSTask>),

    Storage(storage_server::StorageNode),

}
async fn run_sequencer(cfg: Config) -> NodeType {    
    let node = sequencer::SequencerNode::new(cfg);
    
    NodeType::Sequencer(node)
}


async fn run_worker(cfg: PSLWorkerConfig) -> NodeType {
    let node = worker::PSLWorker::<worker::engines::AbortableKVSTask>::new(cfg);

    NodeType::Worker(node)
}

async fn run_storage(cfg: Config) -> NodeType {
    let node = storage_server::StorageNode::new(cfg);
    NodeType::Storage(node)
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
async fn run_main(run_mode: RunMode) -> Result<(), io::Error> {
    let node_type = match run_mode {
        RunMode::Storage(cfg) => run_storage(cfg).await,
        RunMode::Worker(cfg) => run_worker(cfg).await,
        RunMode::Sequencer(cfg) => run_sequencer(cfg).await,
    };

    match node_type {
        NodeType::Sequencer(mut consensus_node) => {
            handle_signal_till_end!(consensus_node);
        },
        NodeType::Worker(mut worker_node) => {
            handle_signal_till_end!(worker_node);
        },
        NodeType::Storage(mut storage_node) => {
            handle_signal_till_end!(storage_node);
        }
    };
    
    
    Ok(())
}

const NUM_THREADS: usize = 16;

fn main() {
    log4rs::init_config(config::default_log4rs_config()).unwrap();

    let run_mode = process_args();

    let (protocol, app) = get_feature_set();
    info!("Protocol: {}, App: {}", protocol, app);


    let core_ids = 
        Arc::new(Mutex::new(Box::pin(core_affinity::get_core_ids().unwrap())));

    let (node_list, my_name) = match &run_mode {
        RunMode::Storage(_cfg) | RunMode::Sequencer(_cfg) => {
            (_cfg.consensus_config.node_list.clone(), _cfg.net_config.name.clone())
        },
        RunMode::Worker(_cfg) => {
            (_cfg.worker_config.all_worker_list.clone(), _cfg.net_config.name.clone())
        },
    };

    let start_idx = 0; // node_list.iter().position(|r| r.eq(&my_name)).unwrap();
    let mut num_threads = NUM_THREADS;
    {
        let _num_cores = core_ids.lock().unwrap().len();
        if _num_cores - 1 < num_threads {
            // Leave one core for the storage compaction thread.
            num_threads = _num_cores - 1;
        }
    }

    let start_idx = start_idx * num_threads;
    
    let i = Box::pin(AtomicUsize::new(0));
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_threads)
        .on_thread_start(move || {
            let _cids = core_ids.clone();
            let lcores = _cids.lock().unwrap();
            let id = (start_idx + i.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) % lcores.len();
            // let res = core_affinity::set_for_current(lcores[id]);
    
            // if res {
            //     debug!("Thread pinned to core {:?}", id);
            // }else{
            //     debug!("Thread pinning to core {:?} failed", id);
            // }
            std::io::stdout().flush()
                .unwrap();
        })
        .build()
        .unwrap();
    let _ = runtime.block_on(run_main(run_mode));
}
