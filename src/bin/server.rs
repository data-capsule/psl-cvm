// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use log::{debug, error, info, warn};
use nix::libc;
use psl::config::{self, Config, PSLWorkerConfig};
use psl::proto::client::ProtoClientRequest;
use psl::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase};
use psl::rpc::server::LatencyProfile;
use psl::rpc::{PinnedMessage, SenderType};
use psl::utils::channel::{make_channel, Sender};
use psl::utils::timer::ResettableTimer;
use psl::worker::TxWithAckChanTag;
use psl::{sequencer, storage_server, worker};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::{runtime, signal};
use std::process::{exit, Command};
use std::time::Duration;
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


async fn prepare_fifo_reader_writer(idx: usize, channel_depth: usize, client_request_tx: Sender<TxWithAckChanTag>) {
    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::channel(channel_depth);

    let __dummy_tx = reply_tx.clone();

    // let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        // Just send "yo" to "/tmp/psl_fifo/psl_fifo_out" for every response.

        // Open in append mode.
        let file = loop {

            match File::options().append(true).create(true)
                // .custom_flags(libc::O_NONBLOCK as i32)
                .open(format!("/tmp/psl_fifo/psl_fifo_out{}", idx)).await {

                    Ok(file) => break file,
                    Err(e) => {
                        error!("Failed to open file: {:?}", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
        };
        
        
        let mut writer = BufWriter::new(file);
        let flush_timer = ResettableTimer::new(Duration::from_secs(1));

        flush_timer.run().await;

        let mut ack_counter = 0usize;

        // let mut ctr = 0;
        loop {
            tokio::select! {
                _ = flush_timer.wait() => {
                    let _ = writer.flush().await;
                }
                Some(_) = reply_rx.recv() => {
                    // timeouwriter.write_all(b"yo\n").await.unwrap();
                    let val = format!("{}\n", ack_counter);       
                    let _ = writer.write_all(val.as_bytes()).await;
                    ack_counter += 1;
                },
                // Ok(_) = eof_rx => {
                //     writer.flush().await.unwrap();
                //     let file = writer.into_inner();
                //     file.sync_all().await.unwrap();
                //     drop(file);
                //     return;
                // }
            }
        }
        __dummy_tx.send((PinnedMessage::from(vec![], 0, SenderType::Anon), LatencyProfile::new())).await.unwrap();
    });

    tokio::spawn(async move {
        // Read "/tmp/psl_fifo/psl_fifo_in"
        // Format: 
        // Read request: R base64_key
        // Write request: W base64_key base64_value


        let file = File::open(format!("/tmp/psl_fifo/psl_fifo_in{}", idx)).await.unwrap();
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        let mut client_tag = 0;
        while let Ok(n) = reader.read_until(b'\n', &mut line).await {
            if n == 0 {
                continue;
            }
            let request = String::from_utf8(line.clone()).unwrap();
            let request = request.split_whitespace().collect::<Vec<&str>>();
            if request.len() < 2 {
                continue;
            }

            let op = request[0];
            client_tag += 1;
            match op {
                "R" => {
                    if request.len() < 2 {
                        error!("Invalid request: {:?}", request);
                        continue;
                    }
                    let key = request[1];
                    let request = ProtoTransaction {
                        on_receive: Some(ProtoTransactionPhase {
                            ops: vec![ProtoTransactionOp {
                                op_type: ProtoTransactionOpType::Read as i32,
                                operands: vec![key.as_bytes().to_vec()],
                            }],
                        }),
                        on_byzantine_commit: None,
                        on_crash_commit: None,
                        is_2pc: false,
                        is_reconfiguration: false,
                    };


                    let tx_with_ack_chan_tag: TxWithAckChanTag = (Some(request), (reply_tx.clone(), client_tag, SenderType::Auth("client1".to_string(), 100 + idx as u64)));
                    client_request_tx.send(tx_with_ack_chan_tag).await.unwrap();
                }
                "W" => {
                    if request.len() < 3 {
                        error!("Invalid request: {:?}", request);
                        continue;
                    }
                    let key = request[1];
                    let value = request[2];
                    let request = ProtoTransaction {
                        on_receive: Some(ProtoTransactionPhase {
                            ops: vec![ProtoTransactionOp {
                                op_type: ProtoTransactionOpType::Write as i32,
                                operands: vec![key.as_bytes().to_vec(), value.as_bytes().to_vec()],
                            }],
                        }),
                        on_byzantine_commit: None,
                        on_crash_commit: None,
                        is_2pc: false,
                        is_reconfiguration: false,
                    };

                    let tx_with_ack_chan_tag: TxWithAckChanTag = (Some(request), (reply_tx.clone(), client_tag, SenderType::Auth("client1".to_string(), 100 + idx as u64)));
                    client_request_tx.send(tx_with_ack_chan_tag).await.unwrap();
                },
                // "E" => {
                //     eof_tx.send(()).unwrap();
                //     break;
                // }
                _ => {
                    warn!("Unknown operation: {}", op);
                }
            }

            line.clear();
        }

        panic!("Drop!");
    });
}

async fn make_all_fifo_files(total: usize) {
    Command::new("mkdir").arg("-p").arg("/tmp/psl_fifo").output().unwrap();
    
    for i in 1..=total {
        Command::new("mkfifo").arg(format!("/tmp/psl_fifo/psl_fifo_in{}", i)).output().unwrap();
        Command::new("mkfifo").arg(format!("/tmp/psl_fifo/psl_fifo_out{}", i)).output().unwrap();
    }

}

async fn run_worker(cfg: PSLWorkerConfig) -> NodeType {
    let (client_request_tx, client_request_rx) = make_channel::<TxWithAckChanTag>(cfg.rpc_config.channel_depth as usize);

    make_all_fifo_files(4).await;
    for i in 1..=4 {
        prepare_fifo_reader_writer(i, cfg.rpc_config.channel_depth as usize, client_request_tx.clone()).await;
    }
    
    
    let node = worker::PSLWorker::<worker::engines::AbortableKVSTask>::mew(cfg, client_request_tx, client_request_rx);

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
