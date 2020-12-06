use crate::apps;
use crate::ds;

use actix::prelude::*;
extern crate ndarray;
use crossbeam_utils::atomic::AtomicCell;
use std::sync::mpsc::{self};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

/*
 *
 * Logic for sender:
 *
 * Loop:
 *
 *   should kill self?
 *     kill self
 *   should check for new schedule?
 *     update/check schedule
 *
 *   select which block for request based on cache simulator
 *   cachesimulator.update
 *   ws.send(block)
 *   if should sleep to manage bandwidth:
 *     sleep
 *
 *   update channel to scheduler
 *
 **/

pub fn start(
    app: Arc<Mutex<Box<dyn apps::AppTrait>>>,
    cache_sim: Arc<RwLock<super::CacheSimulator>>,
    ws_addr: Recipient<ds::StreamBlock>,
    tm: Arc<RwLock<ds::TimeManager>>,
    _congestion: Arc<AtomicCell<u128>>,
    kill_thread: Arc<AtomicCell<bool>>,
    min_wait: usize,
    schedule_rx: Arc<Mutex<mpsc::Receiver<Vec<usize>>>>,
) {
    // stats
    let mut round: usize = 1;
    let mut total_blocks: usize = 1;

    // for rate control

    let mut schedule_pt: Vec<usize> = Vec::new();
    let mut schedule_iter = schedule_pt.iter();

    // for bw control
    let block_size = app.lock().unwrap().get_block_size(); // bytes
    let size_megabits = (block_size as f64 * 8.0) / (1024.0 * 1024.0);
    let bandwidth = tm.read().unwrap().get_ref_bw();
    info!(
        "block_size: {:?} size_megabits: {:?}",
        block_size, size_megabits
    );

    let mut start = Instant::now();
    loop {
        let kill_thread_flag = kill_thread.load();
        if kill_thread_flag {
            debug!("Terminating thread 2 round ({})", round);
            break;
        }

        schedule_iter = match schedule_rx.lock().unwrap().try_recv() {
            Ok(schedule) => {
                debug!("scheduler: {:?}", schedule);
                schedule_pt = schedule;

                // submit this to app
                app.lock().unwrap().prepare_schedule(&schedule_pt);

                schedule_pt.iter()
            }
            _ => schedule_iter,
        };

        match schedule_iter.next() {
            Some(&qid) => {
                // get how many blocks in cache, and update cache
                let incache = cache_sim.read().unwrap().get(qid);
                let cache_start = Instant::now();
                cache_sim.write().unwrap().add(qid);
                let cache_update_time = cache_start.elapsed().as_millis() as u64;
                let retrieval_start = Instant::now();
                let count = 1;
                match app.lock().unwrap().get_nblocks_byindex(qid, count, incache) {
                    Some(blocks) => {
                        if blocks.len() == 0 {
                            // todo: give scheduler max blocks per query
                            error!("get_nblocks no blocks: {:?} {:?} {:?} <- happens when we have var # of blocks", qid, count, incache);
                            continue;
                        }

                        for b in blocks {
                            let retrieval_time = retrieval_start.elapsed().as_millis();
                            let sending_start = Instant::now();
                            let req = ws_addr.send(b);
                            let w = req.wait();
                            match w {
                                Ok(_) => {
                                    total_blocks += 1;
                                    // debug!(
                                    //     "sending took: {:?} retrieval: {:?} cache_update: {:?}",
                                    //     sending_start.elapsed(),
                                    //     retrieval_time,
                                    //     cache_update_time
                                    // );
                                    if sending_start.elapsed().as_millis() > 1 {
                                        error!("congestion {:?}", sending_start.elapsed());
                                    }
                                }
                                Err(e) => {
                                    error!("websocket senderror {:?}", e);
                                    continue;
                                }
                            }
                        }
                    }
                    None => {
                        error!("get_nblocks None: {:?} {:?} {:?}", qid, count, incache);
                        continue;
                    }
                }
            }
            None => {
                // nothing to send
                continue;
            }
        };

        let bw = bandwidth.load();

        // wait for as long as what we have put on network
        let elapsed = start.elapsed();
        let elapsed_ns = elapsed.as_nanos();

        let sending_time_ms: f64 = ((size_megabits / bw as f64) * 1000.0).ceil();
        let sending_time_ns: u128 = (sending_time_ms * 1000000.0).ceil() as u128;

        info!(
            "({}) -> elapsed for {:?} total_blocks {:?}, bw: {:?} sending_time: {:?}",
            round, elapsed, total_blocks, bw, sending_time_ms
        );

        let wait = sending_time_ns as i64 - elapsed_ns as i64;
        info!("wait {:?}", wait as f64 / 1000000.0);
        let wait = std::cmp::max(wait, min_wait as i64);
        std::thread::sleep(std::time::Duration::from_nanos(wait as u64));

        start = Instant::now(); // before sleep to count for that time

        round += 1;
    }
}
