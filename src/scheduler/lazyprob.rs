use crate::scheduler::prob::PointDist;
use crate::scheduler::prob::ProbTrait;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included};
use std::time::Instant;
pub struct LazyProb {
    total_queries: usize,
    probs_t: HashMap<usize, LazyProbInstance>,
    deltas_ms: BTreeSet<usize>,
    inf: f32,
    pub time: Instant,
    num_row: usize,
    point_dist: PointDist,
}

impl LazyProb {
    pub fn new(total_queries: usize) -> Self {
        let probs_t: HashMap<usize, LazyProbInstance> = HashMap::new();
        let deltas_ms = BTreeSet::new();
        let inf: f32 = 1.0 / total_queries as f32;
        let time = Instant::now();
        let num_row = (total_queries as f32).sqrt() as usize;
        let point_dist = PointDist {
            alpha: 1.0,
            q_index: 0,
        };
        LazyProb {
            total_queries: total_queries,
            probs_t: probs_t,
            deltas_ms: deltas_ms,
            inf: inf,
            time: time,
            num_row: num_row,
            point_dist: point_dist,
        }
    }

    pub fn set_point_dist(&mut self, alpha: f64, index: usize) {
        self.point_dist.alpha = alpha as f32;
        self.point_dist.q_index = index;
    }

    pub fn get_linear_prob(&self, key: usize, p: f32) -> f32 {
        self.point_dist.alpha * p + (1.0 - self.point_dist.alpha) * self.point_dist.get_prob(key)
    }

    pub fn set_probs_at(&mut self, probs: LazyProbInstance, delta: usize) {
        self.probs_t.insert(delta, probs);
        self.deltas_ms.insert(delta);
    }

    pub fn set_probs_by_params(
        &mut self,
        delta: usize,
        xmu: f64,
        ymu: f64,
        xsigma: f64,
        ysigma: f64,
    ) {
        let num_row = self.num_row;
        let num_col = num_row;
        let probs = LazyProbInstance::new(xmu, ymu, xsigma, ysigma, num_row, num_col);
        self.set_probs_at(probs, delta);
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

        let p = base * (p0 + slop * ((i as f32 + j as f32) / 2.0 - low as f32));
        if p < 0.0 {
            error!(
                "area is negative: {:?} {} {} {} {} {} {} {}",
                p, low, up, i, j, slop, p0, pm
            );
        }

        p
    }
}

impl ProbTrait for LazyProb {
    fn get_time(&self) -> Instant {
        self.time
    }

    /// get the probability for delta
    /// interpolate between two deltas in the model
    #[inline]
    fn get(&self, key: usize, delta: usize) -> f32 {
        let (low, up) = self.get_time_bounds(delta);
        let p0 = self.get_probs_at(key, low);
        let p1 = self.get_probs_at(key, up);
        let slop = (p1 - p0) / (up - low) as f32;
        let p = p0 + (delta - low) as f32 * slop;
        p
    }

    /// use the given time to query the model
    #[inline]
    fn get_probs_at(&self, key: usize, delta: usize) -> f32 {
        let p = match self.probs_t.get(&delta) {
            Some(probs) => probs.get(key),
            None => self.inf,
        };

        self.get_linear_prob(key, p)
    }

    fn get_center_query_id(&self, delta: usize) -> usize {
        let lower_bound = self.get_lower_bound(delta);
        return match self.probs_t.get(&delta) {
            Some(probs) => probs.get_center_query_id(),
            None => 0,
        };
    }

    fn get_lower_bound(&self, delta_0: usize) -> usize {
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

    #[inline]
    fn integrate_over_range(&self, qid: usize, delta_0: usize, delta_m: usize, low: usize) -> f32 {
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

/// LazyProb is assumed to be gaussian distribution
pub struct LazyProbInstance {
    xmu: f64,
    ymu: f64,
    xsigma: f64,
    ysigma: f64,
    num_row: usize,
    num_col: usize,
}

#[inline]
fn norm_cdf(x: f64, mu: f64, sigma: f64) -> f64 {
    let z: f64 = (x - mu) / sigma;
    const SQRT_2: f64 = 1.4142135623730951;
    let y: f64 = z / SQRT_2;
    let cdf: f64 = {
        if x >= 3.0 {
            0.5 * statrs::function::erf::erfc(-1.0 * y)
        } else {
            0.5 + 0.5 * statrs::function::erf::erf(y)
        }
    };

    cdf
}

impl LazyProbInstance {
    pub fn new(
        xmu: f64,
        ymu: f64,
        xsigma: f64,
        ysigma: f64,
        num_row: usize,
        num_col: usize,
    ) -> Self {
        LazyProbInstance {
            xmu: xmu,
            ymu: ymu,
            xsigma: xsigma,
            ysigma: ysigma,
            num_row: num_row,
            num_col: num_col,
        }
    }

    #[inline]
    pub fn get(&self, key: usize) -> f32 {
        let x = (key / self.num_row) as f64;
        let y = (key % self.num_row) as f64;
        let xpw = norm_cdf(x, self.xmu, self.xsigma);
        let xmw = norm_cdf(x + 1., self.xmu, self.xsigma);
        let yph = norm_cdf(y, self.ymu, self.ysigma);
        let ymh = norm_cdf(y + 1., self.ymu, self.ysigma);
        let prob = xpw * yph - xpw * ymh - xmw * yph + xmw * ymh;
        return prob as f32;
    }

    #[inline]
    pub fn get_center_query_id(&self) -> usize {
        (self.xmu as usize * self.num_row + self.ymu as usize) as usize
    }
}
