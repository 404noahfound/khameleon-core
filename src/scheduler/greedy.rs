#[warn(dead_code)]
use crate::ds;

/// public lib
extern crate rand;
use rand::distributions::Distribution;
use rand::distributions::WeightedIndex;
use rand::Rng;
use std::sync::{Arc, RwLock};
use std::time::Instant;

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
    #[inline]
    pub fn integrate_probs_slow(
        &self,
        probs: super::Prob,
        total_queries: usize,
        horizon: usize,
    ) -> Array2<f32> {
        let mut matrix: Array2<f32> = Array2::zeros((total_queries, horizon));
        let tm = self.tm.read().unwrap();
        let mut deltas: Vec<usize> = Vec::new();
        let mut lows: Vec<usize> = Vec::new();
        for t in 0..horizon {
            deltas.push(tm.slot_to_client_delta(t));
            lows.push(probs.get_lower_bound(t));
        }
        let horizon_delta = tm.slot_to_client_delta(horizon);
        let mut query_id = 0;
        for mut row in matrix.genrows_mut() {
            for (t, v) in row.indexed_iter_mut() {
                *v = probs.integrate_over_range(query_id, deltas[t], horizon_delta, lows[t]);
            }
            query_id += 1;
        }

        matrix
    }

    /// Analytically compute area under linear curve from t to m
    ///
    /// For each query, integrate (e.g., sum) over probabilities
    /// precompute u_i,t array
    ///     i: query
    ///     t: time step
    ///     horizon:  max timestep
    ///     where u_i,t means the sum probabilities from t to m for query i
    #[inline]
    pub fn integrate_probs(
        &self,
        probs: super::Prob,
        total_queries: usize,
        horizon: usize,
    ) -> Array2<f32> {
        let mut matrix: Array2<f32> = Array2::zeros((total_queries, horizon));
        let mut index = 0;

        let tm = self.tm.read().unwrap();
        let mut deltas: Vec<usize> = Vec::new();
        let mut lows: Vec<usize> = Vec::new();
        for t in 0..horizon {
            deltas.push(tm.slot_to_client_delta(t));
            lows.push(probs.get_lower_bound(t));
        }
        let horizon_delta = tm.slot_to_client_delta(horizon);

        let q_in_p = probs.get_k();
        let mut rest: Option<Array1<f32>> = None;

        // iterate over queries in probs and use their explicit probabilites
        // then compute for a uniform
        for mut row in matrix.genrows_mut() {
            if q_in_p.contains(&index) {
                // compute the probability of the query over future timestamps
                for (t, v) in row.indexed_iter_mut() {
                    *v = probs.integrate_over_range(index, deltas[t], horizon_delta, lows[t]);
                }
            } else {
                match &rest {
                    Some(r) => {
                        row.assign(&r.clone());
                    }
                    None => {
                        let mut rest_prob: Array1<f32> = Array1::zeros(horizon);
                        for (t, v) in rest_prob.indexed_iter_mut() {
                            *v = probs.integrate_over_range(
                                index,
                                deltas[t],
                                horizon_delta,
                                lows[t],
                            );
                        }
                        row.assign(&rest_prob.clone());
                        rest = Some(rest_prob);
                    }
                }
            }
            index += 1;
        }

        matrix
    }

    pub fn integrate_probs_partition(
        &self,
        probs: super::Prob,
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
            let dist = match WeightedIndex::new(&rewards) {
                Ok(dist) => dist,
                Err(e) => {
                    error!("{:?} Invalid weight: {:?}", e, p_qids);
                    continue;
                }
            };

            let qindex = dist.sample(&mut rng);
            let qid = {
                if qindex == queries_ids.len() - 1 {
                    let num = rng.gen_range(0, total_queries);
                    num
                } else {
                    queries_ids[qindex]
                }
            };

            // if the qid is last one then pick randomly from all set of queries
            if state[qid] < utility.len() {
                blocks.push(qid);
                state[qid] += 1;
            } else {
                continue;
            }
        }

        blocks
    }

    pub fn sample_plan(
        &self,
        p_qids: &mut ArrayViewMut2<f32>,
        g_qids: ArrayView2<f32>,
        horizon: usize,
        total_queries: usize,
        max_blocks_count: usize,
        mut state: Array1<usize>,
    ) -> Vec<usize> {
        let mut plan: Vec<usize> = Vec::new();
        let epsilon = 0.0; //1e-6;
        let mut rng = rand::thread_rng();

        assert!(g_qids.shape()[0] <= total_queries && g_qids.shape()[1] <= max_blocks_count);
        assert!(p_qids.shape()[0] <= total_queries && p_qids.shape()[1] <= horizon);
        unsafe {
            // for each timestep
            for i in 0..horizon {
                let mut sum: f32 = 0.0;
                for j in 0..total_queries {
                    let nblocks = state[j];
                    // instead fo this, use hashtable max_blocks_per_query
                    //if nblocks < max_blocks_count {
                    if nblocks < self.blocks_per_query[j] {
                        let rewards = g_qids.uget((j, nblocks)) * p_qids.uget((j, i));
                        let mut_qids = p_qids.uget_mut((j, i));
                        *mut_qids = rewards;
                        sum += rewards;
                    } else {
                        let mut_qids = p_qids.uget_mut((j, i));
                        *mut_qids = 0.0;
                    }
                }

                if sum > epsilon {
                    let random = {
                        let mut temp: f32 = 0.0;
                        let rand: f32 = rng.gen();
                        let val: f32 = rand * sum;
                        let mut ret: usize = 1;
                        // accept reject sampling
                        for k in 0..total_queries {
                            if temp < val {
                                ret = k;
                            } else {
                                break;
                            }

                            temp += p_qids.uget((k, i));
                        }

                        ret
                    };

                    // this should be less than the block count for that query
                    if state[random] < max_blocks_count {
                        state[random] += 1;
                        plan.push(random);
                    }
                }
            }
        }

        plan
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

    /**
     * # Arguments
     * * `horizon` - the number of slots to assign in cache
     */
    pub fn make_greedy_plan(
        &self,
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
        let mut rewards: Array1<f32> = Array1::zeros(total_queries);
        for t in 0..horizon {
            let mut sum = 0.0;
            // for each qid, at time t get their probabilities
            let probs_current = prob_matrix.slice_mut(s![..total_queries, t]);
            // get the reward for each query according to how many blocks

            for i in 0..total_queries {
                let nblocks = state[i];
                if nblocks < self.blocks_per_query[i] {
                    rewards[i] = utility[nblocks] * probs_current[i];
                    sum += rewards[i];
                } else {
                    rewards[i] = 0.0;
                }
            }

            if sum <= 0.0 {
                break;
            }

            let qid = self.sample_query_weighted_by_rewards(&rewards, &mut rng);
            if state[qid] < utility.len() {
                blocks.push(qid);
                state[qid] += 1;
            } else {
                continue;
            }
        }

        blocks
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
        probs: super::Prob,
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
            let mut prob_matrix = self.integrate_probs_slow(probs, total_queries, horizon);
            let plan = self.make_greedy_plan(
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
