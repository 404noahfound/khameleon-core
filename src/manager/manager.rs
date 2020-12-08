/*
 * Define Manager Interface that manages
 * two threads, scheduling and streaming
 */
/// local imports
use crate::apps;
use crate::ds;
use crate::scheduler;

use crossbeam_utils::atomic::AtomicCell;
/// public lib
use serde_derive::{Deserialize, Serialize};
use std::io::prelude::*;

use std::sync::mpsc::{self, TrySendError};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Instant;
// for the Actor primitive
use actix::prelude::*;

/// SharedState contains the satates necessary to run application
/// as well as channels to communicate between scheduling and streaming
/// threads.
///
/// kill_thread_flfag: singal to threads end of execution.
/// dist_{tx/rx}: receives client update state and send it to scheduling thread.
/// schedule_{tx/rx}: store the decision made by scheduler and send it to streaming thread.
/// appstate: application configuration received from client.
/// app: instantiation of a new application based on received appstate.
/// threads: handles for current running threads.
pub struct SharedState {
    pub kill_thread_flag: Arc<AtomicCell<bool>>,

    // predictor state
    // set by webserver, unset by manager
    pub dist_tx: Arc<Mutex<mpsc::SyncSender<ds::PredictorState>>>,
    pub dist_rx: Arc<Mutex<mpsc::Receiver<ds::PredictorState>>>,

    // schedule.
    // shared by scheduler and sender
    pub schedule_tx: Arc<Mutex<mpsc::SyncSender<Vec<usize>>>>,
    pub schedule_rx: Arc<Mutex<mpsc::Receiver<Vec<usize>>>>,

    /// pass the sender to the application, which would be
    /// responsible for signaling to the scheduler if state
    /// has changed e.g layout -> reinitialize state
    pub state_change_flag: Arc<RwLock<bool>>,

    pub appstate: ds::AppState,
    pub app: Arc<Mutex<Box<dyn apps::AppTrait>>>,
    pub threads: Vec<Option<thread::JoinHandle<()>>>,
    pub tm: Arc<RwLock<ds::TimeManager>>,

    // prefetch manager
    pub request_count: usize,
    pub timestamp: std::time::Instant,
    pub cache_sim: Arc<RwLock<super::CacheSimulator>>,
}

impl SharedState {
    pub fn new(
        appstate: ds::AppState,
        app: Arc<Mutex<Box<dyn apps::AppTrait>>>,
        state_change_flag: Arc<RwLock<bool>>,
    ) -> Self {
        let kill_thread_flag = Arc::new(AtomicCell::new(false));

        let (dist_tx, dist_rx) = mpsc::sync_channel(1);
        let dist_tx = Arc::new(Mutex::new(dist_tx));
        let dist_rx = Arc::new(Mutex::new(dist_rx));
        let (schedule_tx, schedule_rx) = mpsc::sync_channel(1);
        let schedule_tx = Arc::new(Mutex::new(schedule_tx));
        let schedule_rx = Arc::new(Mutex::new(schedule_rx));

        let threads = Vec::with_capacity(2);
        let latency_init = 100;
        let bw_init = 10.0;

        let time_block_transfer_ms = 1;
        let tm = ds::TimeManager::new(time_block_transfer_ms, latency_init, bw_init);
        let tm = Arc::new(RwLock::new(tm));
        let timestamp = Instant::now();
        let cachesize = appstate.cachesize;
        let (queries_blcount, _) = app.lock().unwrap().get_scheduler_config();
        // let total_queries = queries_blcount.len();
        let total_queries = 1000 * 1000;
        let cache_sim = Arc::new(RwLock::new(super::CacheSimulator::new(
            cachesize,
            total_queries,
        )));

        SharedState {
            kill_thread_flag: kill_thread_flag,
            appstate: appstate,
            app: app,
            threads: threads,
            dist_tx: dist_tx,
            dist_rx: dist_rx,
            schedule_tx: schedule_tx,
            schedule_rx: schedule_rx,
            state_change_flag: state_change_flag,
            tm: tm,
            request_count: 0,
            timestamp: timestamp,
            cache_sim: cache_sim,
        }
    }
}

pub struct Manager {
    pub state: Option<SharedState>,
    pub ws_addr: Option<Recipient<ds::StreamBlock>>,
    pub manager_addr: Option<Addr<Manager>>,
    dist_counter: usize,
    instance: usize,

    pub config: serde_json::Value,
    pub congestion: Option<Arc<AtomicCell<u128>>>,
}

impl Actor for Manager {
    type Context = Context<Self>;
    // method called on actor start
    fn started(&mut self, ctx: &mut Self::Context) {
        info!("Manager actor: started");
        self.manager_addr = Some(ctx.address());
        // create dir if it doesnt exist
        let path = format!("manager_started.flag");

        debug!("write file to signal manager initialized {:?}", path);
        let path = std::path::Path::new(&path);
        let display = path.display();

        let mut file = match std::fs::File::create(path) {
            Ok(file) => file,
            Err(why) => panic!("couldnt create {}: {}", display, why.to_string()),
        };

        match file.write_all("done".as_bytes()) {
            Err(why) => panic!("couldn't write to {}: {}", display, why.to_string()),
            Ok(_) => debug!("successfully wrote to {}", display),
        }
    }
}

/// Actor Model using acitx
/// This message struct to pass websocket address from server to manager
#[derive(Message)]
#[rtype(bool)]
pub struct Connect {
    pub ws_addr: Recipient<ds::StreamBlock>,
    pub congestion: Arc<AtomicCell<u128>>,
}

/// implementation of actor model for `Connect` Message
/// start communication threads to scheduler and stream data to client
impl Handler<Connect> for Manager {
    type Result = bool;

    fn handle(&mut self, msg: Connect, _: &mut Self::Context) -> Self::Result {
        match &self.ws_addr {
            Some(addr) => match addr.do_send(ds::StreamBlock::Stop) {
                Ok(_) => (),
                Err(e) => debug!("error while closing websocket {:?}", e),
            }, // close it
            None => (),
        }

        self.ws_addr = Some(msg.ws_addr);
        self.congestion = Some(msg.congestion);

        true
    }
}

#[derive(Message, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[rtype(bool)]
pub struct SystemStat {
    pub bw: f64,
    pub latency: u32,
}

impl Handler<SystemStat> for Manager {
    type Result = bool;

    fn handle(&mut self, stat: SystemStat, _: &mut Self::Context) -> Self::Result {
        debug!("stat: {:?}", stat);

        match &self.state {
            Some(state) => match state.tm.write() {
                Ok(mut tm) => {
                    tm.update_bandwidth(stat.bw);
                    tm.update_latency(stat.latency as usize);
                }
                Err(e) => error!("couldn't update bandwidth, {:?}", e),
            },
            None => (),
        }

        true
    }
}

#[derive(Message)]
#[rtype(usize)]
pub struct Distributions {
    pub data: String,
}

impl Handler<Distributions> for Manager {
    type Result = usize;

    fn handle(&mut self, msg: Distributions, _: &mut Self::Context) -> Self::Result {
        let userstate: ds::PredictorState = serde_json::from_str(&msg.data).unwrap();

        if let Some(state) = &self.state {
            self.dist_counter += 1;
            // debug!(
            //     "====> Manager Actor got new distribution {:?} -> {:?}",
            //     self.dist_counter, userstate
            // );

            match state.dist_tx.lock() {
                Ok(v) => match v.try_send(userstate) {
                    Err(TrySendError::Full(data)) => {
                        match state.dist_rx.lock() {
                            Ok(rx) => {
                                rx.try_recv();
                            }
                            Err(err) => {
                                error!("Distributions: error dist_rx lock {:?}", err);
                            }
                        }
                        let _ = v.try_send(data);
                    }
                    _ => {}
                },
                Err(_) => {
                    error!("couldn't get hold of channel dist_tx");
                }
            };
        }

        self.dist_counter
    }
}

#[derive(Message, Debug, Serialize, Deserialize)]
#[rtype(bool)]
pub struct Request {
    /// currently, json encoded strings are only supported as queries
    pub query: serde_json::Value,
    pub rtype: bool, // prefetch: 1, request: 0
                     // prefetch or request
}

impl Handler<Request> for Manager {
    type Result = bool;

    fn handle(&mut self, msg: Request, _: &mut Self::Context) -> Self::Result {
        debug!(
            "====> Manager Actor got new direct request {:?} {:?}",
            msg.query, msg.rtype
        );
        // if prefetch, check how many  requests we per second we sent so far and check the
        // available bandwidth
        let ws_addr = match self.ws_addr.clone() {
            Some(addr) => addr,
            None => panic!("websocket address wasn't initialized"),
        };

        match &mut self.state {
            Some(state) => {
                let mut queries = vec![];
                if msg.rtype == true {
                    let mut a: Vec<String> = serde_json::from_value(msg.query).unwrap();
                    queries.append(&mut a);
                } else {
                    let q: String = serde_json::from_value(msg.query).unwrap();
                    queries.push(q);
                }

                let mut ret = false;
                for (_, q) in queries.iter().enumerate() {
                    // for each request
                    let count = 1;
                    let incache = 0;
                    match state
                        .app
                        .lock()
                        .unwrap()
                        .get_nblocks_bykey(&q, count, incache)
                    {
                        Some(blocks) => {
                            for b in blocks {
                                state.request_count += 1;
                                let _ = ws_addr.do_send(b);
                            }
                            ret = true;
                        }
                        None => (),
                    }
                }

                ret
            }
            None => panic!("state is not initialized"),
        }
    }
}

#[derive(MessageResponse, PartialEq)]
pub struct InitAppData {
    pub instance: usize,
    pub data: String,
}

#[derive(Message)]
#[rtype(InitAppData)]
pub struct InitApp {
    pub state: String,
}

impl Handler<InitApp> for Manager {
    type Result = InitAppData;

    fn handle(&mut self, msg: InitApp, _: &mut Self::Context) -> Self::Result {
        match serde_json::from_str(&msg.state) {
            Ok(appstate) => {
                info!("====> Manager Actor to initialize app {:?}", appstate);
                // these should be initialized by the client
                // start scheduler/streaming threads
                match &mut self.state {
                    Some(state) => {
                        debug!("cleaning up old state");
                        // 1) check if any threads is already running -> kill them to end
                        state.kill_thread_flag.store(true);

                        // 2) join thread handles
                        for worker in &mut state.threads {
                            if let Some(thread) = worker.take() {
                                thread.join().unwrap();
                                debug!("joined thread");
                            }
                        }

                        let state_change_flag = Arc::new(RwLock::new(false));
                        let app = state.app.clone();
                        let shstate = SharedState::new(appstate, app, state_change_flag);
                        self.state = Some(shstate);
                    }
                    None => {
                        debug!("initializing new state");
                        // TODO: create this when the application start
                        //       let the user connect to this specific app
                        //       query initialization state
                        //       update cache size available at client side
                        let state_change_flag = Arc::new(RwLock::new(false));

                        let app = Arc::new(Mutex::new(apps::new(
                            &appstate,
                            self.config.clone(),
                            state_change_flag.clone(),
                        )));
                        let shstate = SharedState::new(appstate, app, state_change_flag);

                        match shstate.tm.write() {
                            Ok(mut tm) => {
                                let latency: usize = match self.config["latency"].as_u64() {
                                    Some(l) => l as usize,
                                    None => 100,
                                };
                                let bw = match self.config["bandwidth"].as_f64() {
                                    Some(l) => l,
                                    None => 10.0,
                                };
                                tm.update_bandwidth(bw);
                                tm.update_latency(latency as usize);
                            }
                            Err(e) => error!("couldn't update bandwidth, {:?}", e),
                        }

                        self.state = Some(shstate);
                        self.instance += 1;
                    }
                }
            }
            Err(err) => panic!("invalid app state {:?}", err),
        };

        let state = match &mut self.state {
            Some(state) => state,
            None => panic!("no state initialized"),
        };
        let appinit = state.app.lock().unwrap().get_initstate();

        debug!("running {} threads", state.threads.len());
        InitAppData {
            instance: self.instance,
            data: appinit,
        }
    }
}

#[derive(Message)]
#[rtype(bool)]
pub struct StartThreads;

impl Handler<StartThreads> for Manager {
    type Result = bool;

    fn handle(&mut self, _msg: StartThreads, _: &mut Self::Context) -> Self::Result {
        let run_scheduler: bool = match self.config["runScheduler"].as_bool() {
            Some(flag) => flag,
            None => {
                error!(
                    "runScheduler wasn't initialized; use default {:?}",
                    self.config
                );
                true
            } // by default start scheduler and sender threads
        };

        debug!("run_scheduler: {:?}", run_scheduler);

        if run_scheduler {
            let ws_addr = match self.ws_addr.clone() {
                Some(addr) => addr,
                None => panic!("websocket address wasn't initialized"),
            };

            let state = match &mut self.state {
                Some(state) => state,
                None => panic!("no state initialized"),
            };
            let congestion_flag = match self.congestion.clone() {
                Some(v) => v,
                None => panic!("congestion flag isn't set"),
            };
            Manager::start_threads(state, ws_addr, congestion_flag, &self.config);
        }

        run_scheduler
    }
}

impl Manager {
    // start scheduler thread
    // start streaming thread

    pub fn new(config: serde_json::Value) -> Self {
        Manager {
            ws_addr: None,
            manager_addr: None,
            state: None,
            dist_counter: 0,
            instance: 0,
            congestion: None,
            config: config,
        }
    }

    pub fn start_threads(
        state: &mut SharedState,
        ws_addr: Recipient<ds::StreamBlock>,
        congestion_flag: Arc<AtomicCell<u128>>,
        config: &serde_json::Value,
    ) {
        info!("--> Start Scheduling/streaming Threads");
        let kill_thread_th1 = state.kill_thread_flag.clone();
        let kill_thread_th2 = state.kill_thread_flag.clone();
        let dist_rx = state.dist_rx.clone();

        let schedule_rx_th1 = state.schedule_rx.clone();
        let schedule_rx_th2 = state.schedule_rx.clone();
        let schedule_tx = state.schedule_tx.clone();

        let app1 = Arc::clone(&state.app);
        let app2 = Arc::clone(&state.app);

        let (queries_blcount, utility) = app1.lock().unwrap().get_scheduler_config();
        // let total_queries = queries_blcount.len();
        let total_queries = 1000000;

        let state_change_flag = state.state_change_flag.clone();
        let cachesize = state.appstate.cachesize;
        let cache_sim_th1 = state.cache_sim.clone();
        let cache_sim_th2 = cache_sim_th1.clone();

        let tm = state.tm.clone();
        let tm_th1 = tm.clone();
        let tm_th2 = tm.clone();
        let latency: usize = match config["latency"].as_u64() {
            Some(l) => l as usize,
            None => 100,
        };

        let rate: usize = match config["rate"].as_u64() {
            Some(r) => r as usize,
            None => 0,
        };

        let mut bw = match config["bandwidth"].as_f64() {
            Some(bw) => bw,
            None => 10.0,
        };

        if rate > 0 {
            bw = rate as f64;
        }

        let min_wait: usize = match config["min_wait"].as_u64() {
            Some(w) => w as usize,
            None => 0,
        };

        match state.tm.write() {
            Ok(mut tm) => {
                tm.update_bandwidth(bw);
                tm.update_latency(latency as usize);
            }
            Err(e) => error!("couldn't update bandwidth, {:?}", e),
        }

        info!("bw: {} rate: {} latency: {}", bw, rate, latency);

        // 2) Start a Scheduler Threed, that checks queue
        //    for latest recevied model from client, or use
        //    uniform probabilities to make decisions
        //
        //
        // receive updated distributions and schedule new blocks
        // send new decision to thread2
        let worker1 = thread::spawn(move || {
            let blocks_per_query: Vec<usize> = queries_blcount.iter().map(|(_k, &v)| v).collect();
            let continues = false;
            let schedtype = scheduler::SchedulerType::Greedy;
            let time_to_converge = 300;
            let batch = 100;
            let sched = scheduler::new(
                &schedtype,
                batch,
                cachesize,
                utility,
                blocks_per_query,
                Some(tm.clone()),
            );

            super::scheduling::start(
                // objects
                app1,
                cache_sim_th1,
                sched,
                tm_th1,
                // config
                continues,
                time_to_converge,
                total_queries,
                // flags
                kill_thread_th1,
                state_change_flag,
                // channels
                dist_rx,
                schedule_tx,
                schedule_rx_th1,
            );
        });
        state.threads.push(Some(worker1));
        // receive scheduler's decisions and stream them to end user
        let worker2 = thread::spawn(move || {
            super::sender::start(
                // object
                app2,
                cache_sim_th2,
                ws_addr,
                tm_th2,
                congestion_flag,
                // flags
                kill_thread_th2,
                min_wait,
                // channels
                schedule_rx_th2,
            );
        });
        state.threads.push(Some(worker2));
    }
}
