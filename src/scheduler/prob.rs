use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included};
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Prob {
    /// total queries supported by the app
    total_queries: usize,
    probs_t: HashMap<usize, ProbInstance>,
    deltas_ms: BTreeSet<usize>,
    inf: f32,
    pub time: Instant,
    point_dist: PointDist,
}

#[derive(Clone, Debug)]
pub struct ProbInstance {
    /// queries not included in `dist` uniformly distribution with the (1.0 dist.values().sum())
    rest_dist: f32,
    /// explicit probability for queries indexed by their original index in app
    dist: indexmap::IndexMap<usize, f32>,
}

impl ProbInstance {
    pub fn get(&self, key: usize) -> f32 {
        match self.dist.get(&key) {
            Some(value) => (*value).abs(),
            None => self.rest_dist,
        }
    }

    pub fn get_k(&self) -> HashSet<usize> {
        let queries: HashSet<usize> = self.dist.iter().map(|(&k, _)| k).collect();
        queries
    }
}

#[derive(Clone, Debug)]
pub struct PointDist {
    pub alpha: f32,
    pub q_index: usize,
}

impl PointDist {
    pub fn get_prob(&self, key: usize) -> f32 {
        match key == self.q_index {
            true => 1.0,
            _ => 0.0,
        }
    }
}

impl Prob {
    /// Helper to compute probability for a query at time t, if there are multiple distributions
    /// at various times in the future.
    ///
    /// # Arguments
    ///
    /// * `total_queries`- Total queries the application support.
    pub fn new(total_queries: usize) -> Self {
        // model per timestamp
        let probs_t: HashMap<usize, ProbInstance> = HashMap::new();

        // contain deltas_ms in the model
        let deltas_ms = BTreeSet::new();

        // uniform probability
        let inf: f32 = 1.0 / total_queries as f32;

        // to account for progress of time when quering for probabilites
        let time = Instant::now();
        let point_dist = PointDist {
            alpha: 1.0,
            q_index: 0,
        };

        Prob {
            total_queries: total_queries,
            probs_t: probs_t,
            deltas_ms: deltas_ms,
            inf: inf,
            time: time,
            point_dist: point_dist,
        }
    }

    pub fn get_k(&self) -> HashSet<usize> {
        let mut all_queries: HashSet<usize> = HashSet::new();

        for (_, p) in &self.probs_t {
            all_queries = all_queries.union(&p.get_k()).map(|&k| k).collect();
        }

        all_queries.insert(self.point_dist.q_index);
        all_queries
    }

    /// # Arguments
    ///
    /// * `dist` - {key: query index, value:  prob as f32}. queries not included are assigned
    ///             uniform low probability
    #[inline]
    pub fn set_probs_at(&mut self, dist: indexmap::IndexMap<usize, f32>, delta: usize) {
        // probability of the res tof queries not included in `dist`
        let dist_sum: f32 = dist.values().sum();
        let rest_dist: f32 = (1.0 - dist_sum) / self.total_queries as f32;
        self.probs_t.insert(
            delta,
            ProbInstance {
                rest_dist: rest_dist,
                dist: dist,
            },
        );
        self.deltas_ms.insert(delta);
    }

    pub fn set_point_dist(&mut self, alpha: f64, index: usize) {
        self.point_dist.alpha = alpha as f32;
        self.point_dist.q_index = index;
    }

    /// use the given time to query the model
    #[inline]
    pub fn get_probs_at(&self, key: usize, delta: usize) -> f32 {
        let p = match self.probs_t.get(&delta) {
            Some(probs) => probs.get(key),
            None => self.inf,
        };

        self.get_linear_prob(key, p) // interpolate between point and gaussian distribution
    }

    pub fn get_linear_prob(&self, key: usize, p: f32) -> f32 {
        self.point_dist.alpha * p + (1.0 - self.point_dist.alpha) * self.point_dist.get_prob(key)
    }

    /// get the probability for delta
    /// interpolate between two deltas in the model
    #[inline]
    pub fn get(&self, key: usize, delta: usize) -> f32 {
        let (low, up) = self.get_time_bounds(delta);
        let p0 = self.get_probs_at(key, low);
        let p1 = self.get_probs_at(key, up);
        let slop = (p1 - p0) / (up - low) as f32;
        let p = p0 + (delta - low) as f32 * slop;
        p
    }

    /// get the lower and upper bounds for t
    #[inline]
    fn get_time_bounds(&self, delta: usize) -> (usize, usize) {
        let next = delta + 1;
        let mut iter = self.deltas_ms.range(0..next).rev();
        let low = iter.next().unwrap_or(&delta);
        let mut iter = self.deltas_ms.range(next..);
        let up = iter.next().unwrap_or(&next);

        (*low, *up)
    }

    /// assumption: i < j and (i, j) is within (low, up) range
    /// compute the area under the line (i, py1),(j, py2)
    #[inline]
    pub fn area_under_curve(
        &self,
        qid: usize,
        low: usize,
        up: usize,
        mut i: usize,
        mut j: usize,
    ) -> f32 {
        if i >= j || low > i || j > up || up < low {
            //error!("XXX {} {} {} {}", i, j, low, up);
            return 0.0;
        }

        let mut p0 = self.get_probs_at(qid, low).abs();
        let mut pm = self.get_probs_at(qid, up).abs();
        if p0 > pm {
            // assumption: p0 < pm to correctly compute area of triang and rect
            let temp = pm;
            pm = p0;
            p0 = temp;

            // flip i and j too to query the right part of triang
            let temp = j;
            j = up - (i - low);
            i = up - (temp - low);
        }
        let slop: f32 = (pm - p0) / (up - low) as f32;
        let base = (j - i) as f32;
        // area between under linear curve from t to horizon

        //let px = slop * (j as f32 - low as f32) + p0;
        //let py = slop * (i as f32 - low as f32) + p0;
        //let rect = base * px;
        //let triang = base * (py - px) / 2.0;
        //let p = rect + triang;

        let p = base * (p0 + slop * ((i as f32 + j as f32) / 2.0 - low as f32));
        if p < 0.0 {
            error!(
                "area is negative: {:?} {} {} {} {} {} {} {}",
                p, low, up, i, j, slop, p0, pm
            );
        }

        p
    }

    pub fn get_lower_bound(&self, delta_0: usize) -> usize {
        let mut low = 0;
        // delta_ms: for each state sent from client, delta_ms stores distribtions in x ms in the
        // future
        let mut iter = self
            .deltas_ms
            .range((Included(&low), Included(&delta_0)))
            .rev();

        low = *iter.next().unwrap_or(&delta_0);

        low
    }

    /// given a delta t0 (ms) in the future, compute
    /// the probability until delta tm for qid
    /// assumptopm: delta_m > delta_0
    #[inline]
    pub fn integrate_over_range(
        &self,
        qid: usize,
        delta_0: usize,
        delta_m: usize,
        low: usize,
    ) -> f32 {
        let mut p: f32 = 0.0;
        if delta_0 >= delta_m {
            return 0.0;
        }

        let inf = delta_m + 500; // ms
        let mut low = low;
        let mut upper_delta = delta_m;
        let mut lower_delta = delta_0;
        for &up in self
            .deltas_ms
            .range((Excluded(&delta_0), Included(&delta_m)))
        {
            upper_delta = std::cmp::min(up, delta_m);
            lower_delta = std::cmp::max(delta_0, low);
            p += self.area_under_curve(qid, low, up, lower_delta, upper_delta);
            low = up;

            if delta_m <= upper_delta {
                break;
            }
        }

        if low < delta_m {
            p += self.area_under_curve(qid, low, inf, lower_delta, upper_delta);
        }

        p.abs()
    }
}
