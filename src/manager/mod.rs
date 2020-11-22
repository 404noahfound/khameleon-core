pub mod manager;
pub mod scheduling;
pub mod sender;

// export
pub use manager::{Connect, Distributions, InitApp, Manager, Request, SystemStat};

extern crate ndarray;
use ndarray::Array1;

#[derive(Clone, Debug)]
pub struct CacheSimulator {
    pub cache_per_query: Array1<usize>,
    pub cache: Vec<i32>,
    pub cachesize: usize,
    pub head: usize,
}

impl CacheSimulator {
    pub fn new(cachesize: usize, total_queries: usize) -> Self {
        // simulate ring buffer
        let cache: Vec<i32> = vec![-1; cachesize];
        // cache state for scheduler
        let cache_per_query: Array1<usize> = Array1::zeros(total_queries);
        let head = 0;

        CacheSimulator {
            cachesize: cachesize,
            cache: cache,
            cache_per_query: cache_per_query,
            head: head,
        }
    }

    pub fn get_state(&self) -> (usize, Array1<usize>) {
        // make cache state actual cache
        // and and block per query
        (self.head, self.cache_per_query.clone())
    }

    fn get(&self, qid: usize) -> usize {
        match self.cache_per_query.get(qid) {
            Some(count) => *count,
            None => 0,
        }
    }

    fn reset(&mut self) {
        debug!("reset ------ {:?} {:?}", self.cache, self.head);
        self.head = 0;
        self.cache = vec![-1; self.cachesize];
        self.cache_per_query.fill(0);
    }

    fn add(&mut self, qid: usize) {
        // add new block
        let cur_qid = self.cache[self.head];
        if cur_qid >= 0 {
            self.cache_per_query[cur_qid as usize] -= 1;
        }

        self.cache[self.head] = qid as i32;
        self.cache_per_query[qid] += 1;

        if self.head + 1 >= self.cachesize {
            self.reset()
        } else {
            self.head += 1;
        }
    }
}
