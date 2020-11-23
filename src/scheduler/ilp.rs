use crate::ds;

use lp_modeler::dsl::*;
use lp_modeler::solvers::{GurobiSolver, SolverTrait};
use ndarray::{Array1, Array3};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct ILP {
    pub cachesize: usize,
    pub utility: Array1<f32>,
    pub total_queries: usize,
    pub tm: Arc<RwLock<ds::TimeManager>>,
}

pub fn new(
    cachesize: usize,
    utility: Array1<f32>,
    total_queries: usize,
    tm: Arc<RwLock<ds::TimeManager>>,
) -> ILP {
    ILP {
        cachesize: cachesize,
        utility: utility,
        total_queries: total_queries,
        tm: tm,
    }
}

impl ILP {
    /// precompute big u_i,j,t array
    /// i: query
    /// j: j'th block
    /// t: time step
    ///
    /// u_i, j, t = \sum_{k=1}^m prob(i, k) * g(j)
    ///
    pub fn compute_big_u(
        probs: Box<dyn super::ProbTrait>,
        total_queries: usize,
        cachesize: usize,
        utility: &Array1<f32>,
        tm: Arc<RwLock<ds::TimeManager>>,
    ) -> Array3<f32> {
        let tm = tm.read().unwrap();
        let max_blocks_per_query = utility.len();
        let mut big_u: Array3<f32> =
            Array3::zeros((total_queries, max_blocks_per_query + 1, cachesize));
        for qidx in 0..total_queries {
            let mut p_sums: Array1<f32> = (0..cachesize)
                .map(|k| {
                    let delta = tm.slot_to_client_delta(k);
                    probs.get(qidx, delta)
                })
                .collect();

            let mut p = p_sums.sum();

            for t in 0..cachesize {
                p_sums[t] = p;
                if t < cachesize - 1 {
                    p -= p_sums[t + 1];
                }
            }

            let mut g = utility.clone();
            let g = g.view_mut().into_shape((max_blocks_per_query, 1)).unwrap();
            let p_sums = p_sums.view_mut().into_shape((1, cachesize)).unwrap();
            let rewards = g.dot(&p_sums);
            big_u.slice_mut(s![qidx, ..-1, ..]).assign(&rewards);
        }

        big_u
    }
}

impl super::SchedulerTrait for ILP {
    fn run_scheduler(
        &mut self,
        probs: Box<dyn super::ProbTrait>,
        _state: Array1<usize>,
        _start_idx: usize,
    ) -> Vec<usize> {
        let big_u = ILP::compute_big_u(
            probs,
            self.total_queries,
            self.cachesize,
            &self.utility,
            self.tm.clone(),
        );
        let max_blocks_per_query = self.utility.len();
        let mut problem = LpProblem::new("scheduling", LpObjective::Maximize);

        let mut obj_vec: Vec<LpExpression> = Vec::new();
        let vars: HashMap<(usize, usize, usize), LpBinary> = big_u
            .indexed_iter()
            .map(|(i, &v)| {
                let var = LpBinary::new(&format!("a_{}_{}_{}", i.0, i.1, i.2));
                obj_vec.push(v * var.clone());
                (i, var)
            })
            .collect();
        // define objective function and vars
        println!("length of objection vars: {:?}", obj_vec.len());

        problem += obj_vec.sum();

        // define constraints

        // in each time step, at most 1 block should be sent
        for t in 0..self.cachesize {
            let mut sub_vars: Vec<&LpBinary> = Vec::new();
            for q in 0..self.total_queries {
                for b in 0..max_blocks_per_query {
                    sub_vars.push(vars.get(&(q, b, t)).unwrap());
                }
            }
            problem += sub_vars.sum().le(1);
        }

        // avoid duplicating query's blocks; for each allocation,
        // only allocate unique blocks
        for q in 0..self.total_queries {
            for b in 0..max_blocks_per_query {
                let mut sub_vars: Vec<&LpBinary> = Vec::new();
                for t in 0..self.cachesize {
                    sub_vars.push(vars.get(&(q, b, t)).unwrap());
                }
                problem += sub_vars.sum().le(1);
            }
        }

        //problem.write_lp("problem.lp");
        let solver = GurobiSolver::new();
        let result = solver.run(&problem);

        let mut schedule_at_t: Vec<(usize, usize, usize)> = Vec::new();
        // assert that the solution == cachesize
        match result {
            Ok((_, var_values)) => {
                //println!("Status: {:?}", status);
                for (name, &value) in var_values.iter() {
                    if value == 1.0 {
                        let mut tokens = name.split("_");
                        let _ = tokens.next();
                        let qid = tokens.next().unwrap();
                        let qid = qid.parse::<usize>().unwrap();
                        let b = tokens.next().unwrap();
                        let b = b.parse::<usize>().unwrap();
                        let t = tokens.next().unwrap();
                        let t = t.parse::<usize>().unwrap();
                        schedule_at_t.push((qid, b, t));
                    }
                }
            }
            Err(msg) => println!("{}", msg),
        }

        // TODO: blocks should be scheduled from smaller to larger indices
        schedule_at_t.sort_by(|(_, _, t1), (_, _, t2)| t1.cmp(&t2));
        let schedule: Vec<usize> = schedule_at_t.iter().map(|&(qid, _, _)| qid).collect();

        schedule
    }
}
