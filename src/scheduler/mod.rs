/*
 * Scheduler Interface + common functions.
 * SchedulerType: available schedulers
 * SchedulerTrait: the minimumm interface a scheduler has to implement
 *
 * The scheduler takes as input a utility function and a probability distribution
 * over future requests (default: uniform).
 * It then allocates a finite network bandwidth and client cache across progressively
 * encoded data blocks to maximize the expected utility.
 *
 * Goal: ensuring high quality for high probability requests and hedging
 *       for lower probabilit requests.
 */

pub mod bfs;
pub mod decoders;
pub mod greedy;
pub mod ilp;
pub mod prob;

use crate::ds;

pub use decoders::*;
use ndarray::Array1;
pub use prob::Prob;
use serde_derive::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum SchedulerType {
    Greedy,
    ILP,
    BFS,
}

pub fn discretise_utility(utility: Vec<f32>, max_blocks_count: usize) -> Array1<f32> {
    let utility: Array1<f32> = (0..max_blocks_count)
        .enumerate()
        .map(|(i, _v)| {
            if i == 0 {
                utility[i]
            } else if i >= utility.len() {
                0.0
            } else {
                utility[i] - utility[i - 1]
            }
        })
        .collect();

    utility
}

pub fn new(
    &stype: &SchedulerType,
    batch: usize,
    cachesize: usize,
    utility: Vec<f32>,
    blocks_per_query: Vec<usize>,
    tm: Option<Arc<RwLock<ds::TimeManager>>>,
) -> Box<dyn SchedulerTrait> {
    let tm = match tm {
        Some(tm) => tm,
        None => Arc::new(RwLock::new(ds::TimeManager::new(1, 0, 1.0))),
    };

    let max_blocks_count = blocks_per_query.iter().cloned().max().unwrap_or_else(|| 0);
    let total_queries = blocks_per_query.len();
    // init utility array function and the utility for the queries
    let utility = discretise_utility(utility, max_blocks_count);
    match stype {
        SchedulerType::Greedy => {
            Box::new(greedy::new(batch, cachesize, utility, blocks_per_query, tm))
                as Box<dyn SchedulerTrait>
        }
        SchedulerType::ILP => {
            Box::new(ilp::new(cachesize, utility, total_queries, tm)) as Box<dyn SchedulerTrait>
        }
        SchedulerType::BFS => Box::new(bfs::new(batch, cachesize, utility, blocks_per_query, tm))
            as Box<dyn SchedulerTrait>,
    }
}

pub trait SchedulerTrait: Send + Sync + SchedulerClone {
    /// dist: hashmap[key] -> probability
    /// returns hashmap[key] -> block counts
    fn run_scheduler(&mut self, probs: Prob, state: Array1<usize>, start_idx: usize) -> Vec<usize>;
}

pub trait SchedulerClone {
    fn clone_box(&self) -> Box<dyn SchedulerTrait>;
}

impl<T> SchedulerClone for T
where
    T: 'static + SchedulerTrait + Clone,
{
    fn clone_box(&self) -> Box<dyn SchedulerTrait> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn SchedulerTrait> {
    fn clone(&self) -> Box<dyn SchedulerTrait> {
        self.clone_box()
    }
}
