use crate::apps;
use crate::ds;
use crate::scheduler;

use crossbeam_utils::atomic::AtomicCell;
use std::collections::HashMap;
use std::sync::mpsc::{self, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

pub fn start(
    app: Arc<Mutex<Box<dyn apps::AppTrait>>>,
    cache_sim: Arc<RwLock<super::CacheSimulator>>,
    mut sched: Box<dyn scheduler::SchedulerTrait>,
    tm: Arc<RwLock<ds::TimeManager>>,

    // config
    continues: bool,
    time_to_converge: u128,
    total_queries: usize,

    // flags
    kill_thread: Arc<AtomicCell<bool>>,
    state_change_flag: Arc<RwLock<bool>>,

    // channels
    dist_rx: Arc<Mutex<mpsc::Receiver<ds::PredictorState>>>,
    schedule_tx: Arc<Mutex<mpsc::SyncSender<Vec<usize>>>>,
    schedule_rx_th1: Arc<Mutex<mpsc::Receiver<Vec<usize>>>>,
) {
    // stats
    let mut round: usize = 1;
    // variables memory holder
    // let mut decoded_dist_copy: Box<dyn scheduler::ProbTrait>;
    let block_size = app.lock().unwrap().get_block_size(); // bytes
    let size_megabits = (block_size as f64 * 8.0) / (1024.0 * 1024.0);

    // To estimate how long it takes to transfer a block
    match tm.write() {
        Ok(mut tm_w) => tm_w.update_blocksize_megabits(size_megabits),
        Err(e) => panic!("couldn't update time manager with blocksize {:?}", e),
    }

    let mut last_new_dist = Instant::now();
    let debug_cache = false;
    loop {
        let kill_thread_flag = kill_thread.load();
        if kill_thread_flag {
            debug!("Terminating thread 1 round ({})", round);
            break;
        }

        let start = Instant::now();
        let decoded_dist = {
            match dist_rx.lock().unwrap().try_recv() {
                Ok(dist) => {
                    // new distribution
                    let dist = app.lock().unwrap().decode_dist(dist);
                    // decoded_dist_copy = dist;
                    last_new_dist = Instant::now();
                    tm.write().unwrap().update_time((dist).get_time());

                    dist
                }
                Err(TryRecvError::Disconnected) => {
                    info!("channel dist_rx disconnected");
                    break;
                }
                Err(TryRecvError::Empty) => {
                    // check if we sent the whole schedule
                    if continues && (last_new_dist.elapsed().as_millis() > time_to_converge) {
                        info!("use old distribution {:?}", last_new_dist.elapsed());

                        last_new_dist = Instant::now();
                        Box::new(scheduler::Prob::new(total_queries))
                    } else {
                        continue;
                    }
                }
            }
        };

        info!("-------> Thread 1 round ({}) <--------", round);

        let duration = start.elapsed();
        info!("decoding elapsed time {:?}", duration);

        // 2) get the current state from the sender:
        {
            let state_changed = state_change_flag.read().unwrap();
            if *state_changed {
                cache_sim.write().unwrap().reset();
            }
        }
        {
            let mut flag = state_change_flag.write().unwrap();
            *flag = false;
        }
        let (cache_head, cache_state) = cache_sim.read().unwrap().get_state();

        debug!("schedule for {:?}", cache_head);
        if debug_cache {
            let mut cache_content: HashMap<usize, usize> = HashMap::new();

            for (i, &el) in cache_state.iter().enumerate() {
                if el == 0 {
                    continue;
                }

                cache_content.insert(el, i);
            }

            debug!("schedule content nblocks:index {:?}", cache_content);
        }

        // 4) start scheduling
        let start = Instant::now();
        let decision = sched.run_scheduler(decoded_dist, cache_state, cache_head);
        let duration = start.elapsed();

        info!("decisions elapsed time {:?}", duration);

        round += 1;
        if decision.len() == 0 {
            error!("Empty decision results");
            continue;
        }

        // write result to sender thread
        let local_schedule_tx = schedule_tx.lock().unwrap();
        match local_schedule_tx.try_send(decision) {
            Err(TrySendError::Full(data)) => {
                let _ = schedule_rx_th1.lock().unwrap().try_recv();
                local_schedule_tx.try_send(data).unwrap();
            }
            _ => {}
        }
    }
}
