#[warn(dead_code)]
use crate::ds;

/// public lib
extern crate rand;
use crate::scheduler::prob::ProbTrait;
use rand::distributions::Distribution;
use rand::distributions::WeightedIndex;
use rand::Rng;
use std::sync::{Arc, RwLock};

extern crate ndarray;
use ndarray::{Array1, Array2, ArrayView2, ArrayViewMut2};

#[derive(Clone)]
pub struct GreedyScheduler {
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
) -> GreedyScheduler {
    let total_queries = blocks_per_query.len();
    let max_blocks_count = utility.len();
    // TODO: cost too much for large query space
    let mut utility_matrix: Array2<f32> = Array2::zeros((total_queries, max_blocks_count));

    populate_utility_matrix(&mut utility_matrix, &blocks_per_query, &utility);

    let blocks_per_query: Array1<usize> = blocks_per_query.iter().map(|v| *v).collect();

    GreedyScheduler {
        cachesize: cachesize,
        utility: utility,
        batch: batch,
        total_queries: total_queries,
        utility_matrix: utility_matrix,
        tm: tm,
        blocks_per_query: blocks_per_query,
    }
}

impl GreedyScheduler {
    pub fn integrate_probs_partition(
        &self,
        probs: Box<dyn super::ProbTrait>,
        total_queries: usize,
        horizon: usize,
    ) -> (Array2<f32>, Array1<usize>) {
        let mut rest_index = 0;

        let tm = self.tm.read().unwrap();
        let mut deltas: Vec<usize> = Vec::new();
        let mut lows: Vec<usize> = Vec::new();
        for t in 0..horizon {
            deltas.push(tm.slot_to_client_delta(t));
            lows.push(probs.get_lower_bound(t));
        }
        let horizon_delta = tm.slot_to_client_delta(horizon);

        // queries with explicit probabilities, the rest are uniform
        let q_in_p = probs.get_k();
        // last element stores one id from uniform queries
        let mut queries_ids: Array1<usize> = Array1::zeros(q_in_p.len() + 1);
        // last row stores the uniform probability
        let mut matrix: Array2<f32> = Array2::zeros((q_in_p.len() + 1, horizon));

        // iterate over queries in probs and use their explicit probabilites
        // then compute for a uniform

        for (index, &qindex) in q_in_p.iter().enumerate() {
            let mut row = matrix.row_mut(index);
            for (t, v) in row.indexed_iter_mut() {
                // compute the probability of the query over future timestamps
                *v = probs.integrate_over_range(qindex, deltas[t], horizon_delta, lows[t]);
            }
            queries_ids[index] = qindex;
            if rest_index == qindex {
                rest_index += 1;
            }
        }

        //
        if rest_index < total_queries {
            let mut row = matrix.row_mut(q_in_p.len());
            for (t, v) in row.indexed_iter_mut() {
                *v = probs.integrate_over_range(rest_index, deltas[t], horizon_delta, lows[t]);
            }
            queries_ids[q_in_p.len()] = rest_index;
        }

        (matrix, queries_ids)
    }

    pub fn greedy_partition(
        &self,
        queries_ids: Array1<usize>,
        horizon: usize,
        prob_matrix: &mut Array2<f32>,
        total_queries: usize,
        utility: &Array1<f32>,
        mut state: Array1<usize>,
    ) -> Vec<usize> {
        // state: for each query, how many blocks are scheduled
        // for each block slot in cache, which qid is filling the slot
        let mut blocks: Vec<usize> = Vec::new();
        let mut rng = rand::thread_rng();
        let mut rewards: Array1<f32> = Array1::zeros(queries_ids.len());
        for t in 0..horizon {
            let mut sum = 0.0;
            // for each qid, at time t get their probabilities
            let p_qids = prob_matrix.slice_mut(s![..queries_ids.len(), t]);
            // get the reward for each query according to how many blocks

            for i in 0..p_qids.len() {
                let qid = queries_ids[i];
                let nblocks = state[qid];
                if nblocks < self.blocks_per_query[qid] {
                    rewards[i] = utility[nblocks] * p_qids[i];
                    sum += rewards[i];
                } else {
                    rewards[i] = 0.0;
                }
            }

            if sum <= 0.0 {
                println!("sum = zero {:?}", p_qids);
                break;
            }
            // using rewards as weights, sample from qids

            // if the qid is last one then pick randomly from all set of queries
            let qindex = self.sample_query_weighted_by_rewards(&rewards, &mut rng);
            let qid = {
                if qindex == queries_ids.len() - 1 {
                    let num = rng.gen_range(0, total_queries);
                    num
                } else {
                    queries_ids[qindex]
                }
            };

            if state[qid] < utility.len() {
                blocks.push(qid);
                state[qid] += 1;
            } else {
                continue;
            }
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

impl super::SchedulerTrait for GreedyScheduler {
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
            let (mut prob_matrix, queries_ids) =
                self.integrate_probs_partition(probs, total_queries, horizon);
            let plan = self.greedy_partition(
                queries_ids,
                horizon,
                &mut prob_matrix,
                total_queries,
                &self.utility,
                state,
            );
            plan
        };
        plan
    }
}
