// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use base64::engine::general_purpose;
use base64::Engine as _;
use log::{debug, error, info};
use prost::Message;
use psl::config::{self, PSLWorkerConfig};
use psl::consensus::batch_proposal::TxWithAckChanTag;
use psl::proto::client::ProtoClientReply;
use psl::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase};
use psl::rpc::SenderType;
use psl::utils::channel::{make_channel, Receiver, Sender};
use psl::worker;
use serde::Deserialize;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::{runtime, signal};
use std::process::exit;
use std::sync::atomic::AtomicU64;
use std::{env, fs, io, path, sync::{atomic::AtomicUsize, Arc, Mutex}};
use std::io::Write;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Fetch json config file from command line path.
/// Panic if not found or parsed properly.
fn process_args() -> (PSLWorkerConfig, usize) {
    macro_rules! usage_str {
        () => {
            "\x1b[31;1mUsage: {} path/to/config.json port_number\x1b[0m"
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
    let port = args[2].parse::<usize>().unwrap();

    (cfg, port)
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


#[derive(Deserialize)]
enum Encoding {
    plain,
    base64,
}

#[derive(Deserialize)]
struct Key {
    key: String,
    encoding: Encoding,
}

#[derive(Deserialize)]
struct KeyValue {
    key: String,
    value: String,
    encoding: Encoding,
}

struct SharedState {
    tx_sender: UnboundedSender<TxWithAckChanTag>,
    config: PSLWorkerConfig,
    tag_counter: AtomicU64,
}

async fn send_response(result: &ProtoClientReply, is_write: bool) -> impl Responder {
    if result.reply.is_none() {
        return HttpResponse::InternalServerError().json(serde_json::json!({
            "message": "Bug in the server"
        }));
    }

    // Result map:
    // Receipt -> 200, { "value": "base64_encoded_value", "timestamp": lamport_timestamp } | 404
    // TryAgain -> 500, { "message": "try again" }
    // Anything else -> 500, { "message": "Bug in the server" }

    match &result.reply.as_ref().unwrap() {
        psl::proto::client::proto_client_reply::Reply::Receipt(proto_transaction_receipt) => {
            let result = &proto_transaction_receipt.results.as_ref().unwrap().result;
            if result.is_empty() {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "message": "Bug in the server"
                }));
            }

            if !result[0].success {
                if is_write {
                    return HttpResponse::NotFound().json(serde_json::json!({
                        "message": "Write failed"
                    }));
                } else {
                    return HttpResponse::NotFound().json(serde_json::json!({
                        "message": "Key not found"
                    }));
                }
            }

            let values = &result[0].values;

            if is_write {
                HttpResponse::Accepted().json(serde_json::json!({
                    "message": "Write accepted"
                }))
            } else {
                if values.len() != 2 {
                    return HttpResponse::InternalServerError().json(serde_json::json!({
                        "message": "Bug in the server"
                    }));
                }
                let value = &values[0];
                let timestamp = &values[1].clone().try_into();
                match timestamp {
                    Ok(timestamp) => {
                        HttpResponse::Ok().json(serde_json::json!({
                            "value": general_purpose::STANDARD.encode(value),
                            "timestamp": u64::from_be_bytes(*timestamp),
                        }))
                    },
                    Err(_) => {
                        return HttpResponse::InternalServerError().json(serde_json::json!({
                            "message": "Bug in the server"
                        }));
                    }
                }
            }
        },
        psl::proto::client::proto_client_reply::Reply::TryAgain(proto_try_again) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "message": "try again"
            }))
        },
        _ => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "message": "Bug in the server"
            }))
        }
    }
}

#[get("/")]
async fn http_get(data: web::Data<SharedState>, key: web::Json<Key>) -> impl Responder {
    let key = match key.encoding {
        Encoding::plain => key.key.clone().into_bytes(),
        Encoding::base64 => general_purpose::STANDARD.decode(&key.key).unwrap(),
    };
    let tx = ProtoTransaction {
        on_receive: Some(ProtoTransactionPhase {
            ops: vec![
                ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Read as i32,
                    operands: vec![key],
                }
            ]
        }),
        on_crash_commit: None,
        on_byzantine_commit: None,
        is_reconfiguration: false,
        is_2pc: false,
    };
    let tag = data.tag_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let sender = data.config.net_config.name.clone();
    let sender = SenderType::Auth(sender, 0);

    let (act_tx, mut act_rx) = tokio::sync::mpsc::channel(1);
    data.tx_sender.send((Some(tx), (act_tx, tag, sender))).unwrap();

    let result = act_rx.recv().await.unwrap();
    let result = result.0.as_ref();

    let result = ProtoClientReply::decode(&result.0.as_slice()[0..result.1]).unwrap();
    send_response(&result, false).await
}

#[post("/")]
async fn http_post(data: web::Data<SharedState>, key_value: web::Json<KeyValue>) -> impl Responder {

    let key = match key_value.encoding {
        Encoding::plain => key_value.key.clone().into_bytes(),
        Encoding::base64 => general_purpose::STANDARD.decode(&key_value.key).unwrap(),
    };
    let value = match key_value.encoding {
        Encoding::plain => key_value.value.clone().into_bytes(),
        Encoding::base64 => general_purpose::STANDARD.decode(&key_value.value).unwrap(),
    };
    let tx = ProtoTransaction {
        on_receive: Some(ProtoTransactionPhase {
            ops: vec![
                ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Write as i32,
                    operands: vec![key, value],
                }
            ]
        }),
        on_crash_commit: None,
        on_byzantine_commit: None,
        is_reconfiguration: false,
        is_2pc: false,
    };
    let tag = data.tag_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let sender = data.config.net_config.name.clone();
    let sender = SenderType::Auth(sender, 0);

    let (act_tx, mut act_rx) = tokio::sync::mpsc::channel(1);
    data.tx_sender.send((Some(tx), (act_tx, tag, sender))).unwrap();

    let result = act_rx.recv().await.unwrap();
    let result = result.0.as_ref();

    let result = ProtoClientReply::decode(&result.0.as_slice()[0..result.1]).unwrap();

    send_response(&result, true).await
}


async fn run_main(cfg: PSLWorkerConfig, client_request_tx: UnboundedSender<TxWithAckChanTag>, client_request_rx: UnboundedReceiver<TxWithAckChanTag>) -> Result<(), io::Error> {
    let mut node = worker::PSLWorker::<worker::app::KVSTask>::mew(cfg, client_request_tx.clone(), client_request_rx);
    handle_signal_till_end!(node);

    Ok(())
}

async fn run_actix_server(client_request_tx: UnboundedSender<TxWithAckChanTag>, port: usize, config: PSLWorkerConfig) -> Result<(), io::Error> {
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(SharedState {
                tx_sender: client_request_tx.clone(),
                config: config.clone(),
                tag_counter: AtomicU64::new(0),
            }))
            .service(http_get)
            .service(http_post)
    });

    server.bind(format!("0.0.0.0:{}", port)).unwrap().run().await?;

    Ok(())
}

const NUM_THREADS: usize = 32;

fn main() {
    log4rs::init_config(config::default_log4rs_config()).unwrap();

    let (cfg, port) = process_args();

    let (protocol, app) = get_feature_set();
    info!("Protocol: {}, App: {}", protocol, app);

    let core_ids = 
        Arc::new(Mutex::new(Box::pin(core_affinity::get_core_ids().unwrap())));

    let mut num_threads = NUM_THREADS;
    {
        let _num_cores = core_ids.lock().unwrap().len();
        if _num_cores - 1 < num_threads {
            // Leave one core for the storage compaction thread.
            num_threads = _num_cores - 1;
        }
    }

    let (client_request_tx, client_request_rx) = unbounded_channel();

    let i = Box::pin(AtomicUsize::new(0));
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_threads / 2)
        .on_thread_start(move || {
            let _cids = core_ids.clone();
            let lcores = _cids.lock().unwrap();
            let id = (i.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) % lcores.len();
            let res = core_affinity::set_for_current(lcores[id]);
    
            if res {
                debug!("Thread pinned to core {:?}", id);
            }else{
                debug!("Thread pinning to core {:?} failed", id);
            }
            std::io::stdout().flush()
                .unwrap();
        })
        .build()
        .unwrap();
    let _ = runtime.spawn(run_main(cfg.clone(), client_request_tx.clone(), client_request_rx));
    
    let frontend_runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_threads / 2) 
        .build()
        .unwrap();
    match frontend_runtime.block_on(run_actix_server(client_request_tx, port, cfg.clone())) {
        Ok(_) => println!("Frontend server ran successfully."),
        Err(e) => eprintln!("Frontend server error: {:?}", e),
    };
}
