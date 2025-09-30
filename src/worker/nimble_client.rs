use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, crypto::{hash, AtomicKeyStore, HashType}, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}};
use serde::Serialize;
use tokio::sync::Mutex;
use log::{debug, error, info, warn};
use std::{collections::HashMap, pin::Pin, sync::Arc, time::{Duration, Instant}};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};

pub struct NimbleClient {
    config: AtomicPSLWorkerConfig,
    keystore: AtomicKeyStore,
    nimble_rx: Receiver<(Sender<()>, Vec<u8>)>,
    nimble_endpoint_url: String,

    current_counter: usize,

    handle: String,
    client: reqwest::Client,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    retry_count_total: usize,
    num_requests: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct NimblePayload {
    counter: usize,
    state_hash: Vec<u8>,
}


#[derive(Serialize)]
struct CreateRequest {
    Tag: String,
}

#[derive(Serialize)]
struct IncrementRequest {
    Tag: String,
    ExpectedCounter: u64,
}

#[derive(Serialize)]
struct ReadRequest {
    Tag: String,
}

impl NimbleClient {
    pub fn new(
        config: AtomicPSLWorkerConfig, keystore: AtomicKeyStore,
        nimble_rx: Receiver<(Sender<()>, Vec<u8>)>
    ) -> Self {
        // let my_name = &config.get().net_config.name;
        // let pub_key = keystore.get().get_pubkey(my_name).unwrap().clone();
        let handle_bytes = b"nimble_kvs".to_vec();
        // handle_bytes.extend_from_slice(pub_key.as_bytes());
        let handle = hash(&handle_bytes);
        let handle = URL_SAFE.encode(handle.as_slice());
        let nimble_endpoint_url = config.get().get_nimble_endpoint_url();
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));

        let client = reqwest::Client::new();
        Self {
            config, keystore, nimble_rx,
            nimble_endpoint_url,
            current_counter: 0,
            handle,
            client,
            log_timer,

            retry_count_total: 0,
            num_requests: 0,
        }
    }

    pub async fn run(client: Arc<Mutex<Self>>) -> Option<()> {
        info!("NimbleClient started");
        let mut client = client.lock().await;
        client.log_timer.run().await;

        let my_name = &client.config.get().net_config.name;
        if my_name == "node1" {
            client.propose_new_counter(vec![0u8; 32]).await;
        }

        loop {
            tokio::select! {
                Some((tx, new_hash)) = client.nimble_rx.recv() => {
                    client.handle_nimble_commit(tx, new_hash).await;
                },
                _ = client.log_timer.wait() => {
                    client.log_stats().await;
                }
            }
        }

        Some(())
    }

    async fn log_stats(&mut self) {
        let avg_retry_count = if self.num_requests > 0 { self.retry_count_total as f64 / self.num_requests as f64 } else { 0.0 };
        info!("Avg retry count: {}, Num requests: {}", avg_retry_count, self.num_requests);
    }

    async fn handle_nimble_commit(&mut self, tx: Sender<()>, new_hash: Vec<u8>) {
        self.num_requests += 1;
        self.retry_count_total += self.propose_new_counter(new_hash).await;
        let _ = tx.send(()).await;
    }

    async fn propose_new_counter(&mut self, hash: HashType) -> usize {
        let mut retry_count = 0;
        loop {
            retry_count += 1;
            let res = self.__propose_new_counter(hash.clone()).await;
            if res.is_ok() {
                break;
            }

            if res.unwrap_err() {
                self.current_counter += 1;
            }

        }

        retry_count
    }


    async fn __propose_new_counter(&mut self, hash: HashType) -> Result<(), bool> {
        let payload = NimblePayload {
            counter: self.current_counter,
            state_hash: hash,
        };

        let payload_bytes = bincode::serialize(&payload).unwrap();

        let payload_signature = self.keystore.get().sign(&payload_bytes);

        let mut tag = Vec::new();
        tag.extend_from_slice(&payload_bytes);
        tag.extend_from_slice(&payload_signature);

        self.send_to_nimble(tag).await

    }


    async fn send_to_nimble(&mut self, tag: Vec<u8>) -> Result<(), bool> {
        let addr = format!("{}/counters/{}", self.nimble_endpoint_url, self.handle);
        let request = match self.current_counter {
            0 => {
                let json = CreateRequest {
                    Tag: URL_SAFE.encode(tag),
                };

                self.client.put(addr) // PUT => Create
                    .json(&json)


            },
            _ => {
                let json = IncrementRequest {
                    Tag: URL_SAFE.encode(tag),
                    ExpectedCounter: self.current_counter as u64,
                };

                self.client.post(addr) // POST => Increment
                    .json(&json)
            },
        };

        let res = request.send().await;
        if res.is_err() {
            error!("failed to send request to nimble: {:?}", res);
            return Err(false);
        }

        let res = res.unwrap();
        if res.status() != reqwest::StatusCode::OK && res.status() != reqwest::StatusCode::CREATED {
            warn!("Nimble error: {:?}", res);
            if res.status() == 409 {
                return Err(true);
            }
            return Err(false);
        }

        Ok(())

    }
}
