use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::{Duration, Instant}};

use log::info;

use crate::utils::{channel::Receiver, timer::ResettableTimer};

pub enum ClientWorkerStat {
    CrashCommitLatency(Duration),
    ByzCommitLatency(Duration),
    ByzCommitPending(usize /* client_id */, usize /* pending size */),
    SignedAE,
}

pub struct ClientStatLogger {
    stat_rx: Receiver<ClientWorkerStat>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    average_window: Duration,
    max_duration: Duration,

    crash_commit_latency_window: VecDeque<(Instant /* when it was registered */, Duration /* latency value */)>,

    byz_commit_latency_window: VecDeque<(Instant, Duration)>,

    byz_commit_pending_per_worker: HashMap<usize, usize>,

    total_signed_aes: usize,


    benchmark_total_latency: Duration,
    benchmark_total_requests: usize,
    currently_benchmarking: bool,
    ramp_up_timer: Arc<Pin<Box<ResettableTimer>>>,
    ramp_down_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl ClientStatLogger {
    pub fn new(stat_rx: Receiver<ClientWorkerStat>, interval: Duration, average_window: Duration, max_duration: Duration, ramp_up_time: Duration, ramp_down_time: Duration) -> Self {
        let log_timer = ResettableTimer::new(interval);
        let ramp_up_timer = ResettableTimer::new(ramp_up_time);

        let _ramp_down_when = max_duration - ramp_down_time;
        assert!(max_duration > ramp_down_time);
        assert!(_ramp_down_when > ramp_up_time);

        let ramp_down_timer = ResettableTimer::new(_ramp_down_when);
        
        Self {
            stat_rx,
            log_timer,
            average_window,
            crash_commit_latency_window: VecDeque::new(),
            byz_commit_latency_window: VecDeque::new(),
            byz_commit_pending_per_worker: HashMap::new(),
            max_duration,
            total_signed_aes: 0,

            benchmark_total_latency: Duration::from_secs(0),
            benchmark_total_requests: 0,
            currently_benchmarking: false,
            ramp_up_timer,
            ramp_down_timer,
        }
    }

    pub async fn run(&mut self) {
        self.log_timer.run().await;
        self.ramp_up_timer.run().await;
        self.ramp_down_timer.run().await;

        let logger_start_time = Instant::now();

        let mut benchmark_state = 0; // 0: not benchmarking, 1: benchmarking, 2: done.

        while logger_start_time.elapsed() < self.max_duration {

            tokio::select! {
                _ = self.log_timer.wait() => {
                    self.log_stats();
                }
                _ = self.ramp_up_timer.wait() => {
                    if benchmark_state == 0 {
                        self.currently_benchmarking = true;
                        info!("Ramp up complete, starting benchmark");
                        benchmark_state = 1;
                    }

                    // Ignore other ticks.
                }
                _ = self.ramp_down_timer.wait() => {
                    if benchmark_state == 1 {
                        self.currently_benchmarking = false;
                        info!("Starting ramp down, stopping benchmark");
                        benchmark_state = 2;
                    }

                    // Ignore other ticks.
                }
                entry = self.stat_rx.recv() => {
                    if let Some(stat) = entry {
                        self.collect_stat(stat);
                    } else {
                        break;
                    }
                }
            }
        }

        info!("Logging over after {} s", logger_start_time.elapsed().as_secs());
        self.log_stats();

    }

    fn collect_stat(&mut self, stat: ClientWorkerStat) {
        match stat {
            ClientWorkerStat::CrashCommitLatency(latency) => {
                while let Some((registered_time, _latency)) = self.crash_commit_latency_window.front() {
                    if registered_time.elapsed() > self.average_window {
                        self.crash_commit_latency_window.pop_front();
                    } else {
                        break;
                    }
                }
                self.crash_commit_latency_window.push_back((Instant::now(), latency));

                if self.currently_benchmarking {
                    self.benchmark_total_latency += latency;
                    self.benchmark_total_requests += 1;
                }
            }
            ClientWorkerStat::ByzCommitLatency(latency) => {
                while let Some((registered_time, _latency)) = self.byz_commit_latency_window.front() {
                    if registered_time.elapsed() > self.average_window {
                        self.byz_commit_latency_window.pop_front();
                    } else {
                        break;
                    }
                }
                self.byz_commit_latency_window.push_back((Instant::now(), latency));
            }
            ClientWorkerStat::ByzCommitPending(id, pending) => {
                self.byz_commit_pending_per_worker.insert(id, pending);
            },
            ClientWorkerStat::SignedAE => {
                self.total_signed_aes += 1;
            }
        }
    }

    fn log_stats(&mut self) {
        let crash_commit_avg = if self.crash_commit_latency_window.len() > 0 {
            self.crash_commit_latency_window.iter()
                .fold(Duration::from_secs(0), |acc, (_, latency)| acc + *latency)
                .as_secs_f64() / self.crash_commit_latency_window.len() as f64
        } else {
            0.0
        };
        let byz_commit_avg = if self.byz_commit_latency_window.len() > 0 {
            self.byz_commit_latency_window.iter()
                .fold(Duration::from_secs(0), |acc, (_, latency)| acc + *latency)
                .as_secs_f64() / self.byz_commit_latency_window.len() as f64
        } else {
            0.0
        };

        info!("Average Crash commit latency: {} us, Average Byz commit latency: {} us",
            (crash_commit_avg * 1.0e+6) as u64,
            (byz_commit_avg * 1.0e+6) as u64
        );

        let total_pending = self.byz_commit_pending_per_worker.iter().map(|(_, pending)| *pending).sum::<usize>();
        let max_pending = self.byz_commit_pending_per_worker.iter().map(|(_, pending)| *pending).max().unwrap_or(0);
        let min_pending = self.byz_commit_pending_per_worker.iter().map(|(_, pending)| *pending).min().unwrap_or(0);

        info!("Total Byz commit pending: {}, Max pending: {}, Min pending: {}", total_pending, max_pending, min_pending);
    
        if self.total_signed_aes > 0 {
            info!("Total signed requests: {}", self.total_signed_aes);
        }

        let avg_benchmark_latency = if self.benchmark_total_requests > 0 {
            self.benchmark_total_latency.as_secs_f64() / self.benchmark_total_requests as f64
        } else {
            0.0
        };

        info!("Average benchmark latency: {} us Total benchmarked requests: {} Benchmark state: {}",
            (avg_benchmark_latency * 1.0e+6) as u64,
            self.benchmark_total_requests, 
            if self.currently_benchmarking { "Running" } else { "Stopped" }
        );
    }
}


