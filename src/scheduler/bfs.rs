#[warn(dead_code)]
use crate::ds;

extern crate ordered_float;
/// public lib
extern crate rand;
use crate::scheduler::prob::ProbTrait;
use ordered_float::NotNan;
use rand::distributions::Distribution;
use rand::distributions::WeightedIndex;
use rand::Rng;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

extern crate ndarray;
use ndarray::{Array1, Array2, ArrayView2, ArrayViewMut2};

#[derive(Clone)]
pub struct BFSScheduler {
    /// longest future, client cache size in blocks
    pub cachesize: usize,
    /// use indexmap instead?
    pub utility: Array1<f32>,
    pub blocks_per_query: Array1<usize>,
    pub utility_matrix: Array2<f32>,
    /// query space size
    pub total_queries: usize,
    pub tm: Arc<RwLock<ds::TimeManager>>,
    /// the num of blocks assigned in each scheduler run
    pub batch: usize,

    num_queries_searched: usize,
}

fn populate_utility_matrix(
    utility_matrix: &mut Array2<f32>,
    blocks_per_query: &Vec<usize>,
    utility: &Array1<f32>,
) {
    ndarray::Zip::from(utility_matrix.genrows_mut())
        .and(blocks_per_query)
        .apply(|mut a_row, b_elt| {
            for (i, v) in a_row.indexed_iter_mut() {
                if i < *b_elt {
                    *v = utility[i];
                } else {
                    *v = 0.0;
                }
            }
        });
}

pub fn new(
    batch: usize,
    cachesize: usize,
    utility: Array1<f32>,
    blocks_per_query: Vec<usize>,
    tm: Arc<RwLock<ds::TimeManager>>,
) -> BFSScheduler {
    let total_queries = blocks_per_query.len();
    let max_blocks_count = utility.len();
    // TODO: cost too much for large query space
    let mut utility_matrix: Array2<f32> = Array2::zeros((total_queries, max_blocks_count));

    populate_utility_matrix(&mut utility_matrix, &blocks_per_query, &utility);

    let blocks_per_query: Array1<usize> = blocks_per_query.iter().map(|v| *v).collect();

    BFSScheduler {
        cachesize: cachesize,
        utility: utility,
        batch: batch,
        total_queries: total_queries,
        utility_matrix: utility_matrix,
        tm: tm,
        blocks_per_query: blocks_per_query,
        num_queries_searched: 100,
    }
}

impl BFSScheduler {
    #[inline]
    fn neighbours(&self, query_id: usize) -> Vec<usize> {
        const NEIGHBOUR_DELTAS: [[i32; 2]; 4] = [[1, 0], [-1, 0], [0, 1], [0, -1]];
        let mut neighbours: Vec<usize> = Vec::new();
        let n = (self.total_queries as f32).sqrt() as i32;
        let x0 = (query_id as i32) / n;
        let y0 = (query_id as i32) % n;
        for delta in &NEIGHBOUR_DELTAS {
            let x1 = x0 + delta[0];
            let y1 = y0 + delta[1];
            if 0 <= x1 && x1 < n && 0 <= y1 && y1 < n {
                neighbours.push((x1 * n + y1) as usize)
            }
        }
        neighbours
    }

    #[inline]
    fn get_deltas_and_lower_bounds(
        &self,
        probs: &Box<dyn super::ProbTrait>,
        horizon: usize,
        tm: &std::sync::RwLockReadGuard<ds::TimeManager>,
    ) -> (Vec<usize>, Vec<usize>) {
        let mut deltas: Vec<usize> = Vec::new();
        let mut lows: Vec<usize> = Vec::new();
        for t in 0..horizon {
            deltas.push(tm.slot_to_client_delta(t));
            lows.push(probs.get_lower_bound(t));
        }
        (deltas, lows)
    }

    fn bfs_by_rewards(
        &self,
        probs: &Box<dyn super::ProbTrait>,
        query_num_blocks_in_cache: &Array1<usize>,
        deltas: usize,
        horizon_delta: usize,
        lower_bound: usize,
        rewards: &mut Array1<f32>,
        query_ids: &mut Array1<usize>,
        start_id: usize,
    ) {
        let mut visited_ids = HashSet::new();
        let mut next_query_ids = BinaryHeap::new();

        // Insert the start point idx
        let start_prob = probs.integrate_over_range(start_id, deltas, horizon_delta, lower_bound);
        visited_ids.insert(start_id);
        next_query_ids.push((NotNan::new(start_prob).unwrap(), start_id));

        let mut reward_idx = 0;
        while !next_query_ids.is_empty() && reward_idx < self.num_queries_searched {
            let (next_prob, next_query_id) = next_query_ids.pop().unwrap();
            rewards[reward_idx] = *next_prob;
            query_ids[reward_idx] = next_query_id;
            reward_idx += 1;
            let neighbours = self.neighbours(next_query_id);
            // debug!("neighbours for {:?} are {:?}", next_query_id, neighbours);
            for neighbour_id in neighbours {
                if !visited_ids.contains(&neighbour_id) {
                    let neighbour_prob = probs.integrate_over_range(
                        neighbour_id,
                        deltas,
                        horizon_delta,
                        lower_bound,
                    );
                    visited_ids.insert(neighbour_id);
                    next_query_ids.push((NotNan::new(neighbour_prob).unwrap(), neighbour_id));
                }
            }
        }

        for idx in 0..reward_idx {
            rewards[idx] *= self.utility[query_num_blocks_in_cache[query_ids[idx]]];
        }
    }

    fn generate_bfs_plan(
        &self,
        probs: Box<dyn super::ProbTrait>,
        horizon: usize,
        mut query_num_blocks_in_cache: Array1<usize>,
    ) -> Vec<usize> {
        let tm = self.tm.read().unwrap();
        let (deltas, lows) = self.get_deltas_and_lower_bounds(&probs, horizon, &tm);
        let horizon_delta = tm.slot_to_client_delta(horizon);
        let mut rewards: Array1<f32> = Array1::zeros(self.num_queries_searched);
        let mut query_ids: Array1<usize> = Array1::zeros(self.num_queries_searched);
        let mut rng = rand::thread_rng();
        let mut blocks: Vec<usize> = Vec::new();
        for t in 0..horizon {
            self.bfs_by_rewards(
                &probs,
                &query_num_blocks_in_cache,
                deltas[t],
                horizon_delta,
                lows[t],
                &mut rewards,
                &mut query_ids,
                1,
            );
            // debug!("rewards: {:?}, query_ids: {:?}", rewards, query_ids);
            let qindex = self.sample_query_weighted_by_rewards(&rewards, &mut rng);
            query_num_blocks_in_cache[query_ids[qindex]] += 1;
            blocks.push(query_ids[qindex]);
        }
        blocks
    }

    fn sample_query_weighted_by_rewards(
        &self,
        rewards: &Array1<f32>,
        rng: &mut rand::prelude::ThreadRng,
    ) -> usize {
        // using rewards as weights, sample from qids
        let dist = match WeightedIndex::new(rewards) {
            Ok(dist) => dist,
            Err(e) => {
                error!("{:?}", e);
                return rewards.len(); // use len as invalid value
            }
        };
        dist.sample(rng)
    }
}

impl super::SchedulerTrait for BFSScheduler {
    /// start scheduling process.
    /// Implementation of Greedy_P scheduler. In each step, use current state to get prob * utility
    /// for next block for each query, normalize into a prob distribution, and sample
    ///
    /// input: hashmap between queries and their probabilities
    /// output: hashmap between queries and how many blocks should be assigned to them
    ///
    /// TODO: ADD BATCHES TO ACCOUNT FOR LARGE BUFFER SIZE + STATE + MANAGE DISTRIBUTION
    ///       REPRESENTATION
    /// * `start_idx` - next available slot idx in cache
    fn run_scheduler(
        &mut self,
        probs: Box<dyn super::ProbTrait>,
        state: Array1<usize>,
        start_idx: usize,
    ) -> Vec<usize> {
        let total_queries = self.total_queries;
        // dist indexed using the same index in queries vector
        let horizon = std::cmp::min(self.cachesize - start_idx, self.batch);

        if total_queries == 0 {
            return Vec::new();
        }

        let plan: Vec<usize> = {
            // for each query, and for each slot in cache, store the probability of that query
            // let (mut prob_matrix, queries_ids) =
            //     self.integrate_probs_partition(probs, total_queries, horizon);
            // let plan = self.greedy_partition(
            //     queries_ids,
            //     horizon,
            //     &mut prob_matrix,
            //     total_queries,
            //     &self.utility,
            //     state,
            // );
            let plan = self.generate_bfs_plan(probs, horizon, state);
            debug!("blocks sent: {:?}", plan);
            plan
        };
        plan
    }
}
