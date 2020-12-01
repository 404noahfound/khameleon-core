use super::lazyprob::LazyProb;
use super::prob::Prob;
use ndarray::{Array1, Array2, ArrayView1};
use serde_derive::{Deserialize, Serialize};
extern crate statrs;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinearPointGaussian {
    pub p: serde_json::Value,
    pub g: serde_json::Value,
}

/// Map the received probability of queries in json format to a hashmap
/// dist: json format with key as query requested and value as
///       the equivalent probability of that key
///
/// # Example
/// ```
/// let dist = json!({"x": 0.4, "y": 0.6});
/// let decoded_dist = decode_dist(dist);
/// ```
pub fn decode_dist(
    dist: serde_json::Value,
    queries_blcount: &indexmap::IndexMap<String, usize>,
) -> indexmap::IndexMap<usize, f32> {
    let mut map: indexmap::IndexMap<usize, f32> = indexmap::IndexMap::new();
    if let Some(obj) = dist.as_object() {
        for (k, v) in obj.iter() {
            match queries_blcount.get_full(k) {
                Some((index, _, _)) => {
                    let prob = v.as_f64().unwrap() as f32;
                    map.insert(index, prob);
                }
                None => error!("key isn't in queries_blcount {:?}", k),
            }
        }
    }

    map
}

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

/// TODO: think of an optimization that compute queries that fall within
///       percentile range and exclude the others
fn cdf_array(arr: ArrayView1<f32>, mu: f64, sigma: f64) -> Array1<f32> {
    let mut cdf_out = Array1::zeros(arr.len());

    for (i, value) in cdf_out.indexed_iter_mut() {
        *value = norm_cdf(arr[i] as f64, mu, sigma) as f32;
    }

    cdf_out
}

pub fn decode_point_model(point: &serde_json::Value) -> (f64, f64, f64) {
    match point.as_object() {
        Some(obj) => {
            let alpha = obj["a"].as_f64().unwrap_or(1.0);
            let x = obj["X"].as_f64().unwrap_or(0.0);
            let y = obj["Y"].as_f64().unwrap_or(0.0);

            // map x and y to query
            (alpha, x, y)
        }
        None => (1.0, 0.0, 0.0),
    }
}
// pub fn decode_lazy_model(dist: &serde_json::Value, layout_matrix: &Array2<f32>) -> Prob {

// }

/// this is used if we stream model instead of explicit probs
/// get list of queries and their layout -> for each query, compute prob given the layout
#[allow(dead_code)]
pub fn decode_model(dist: &serde_json::Value, layout_matrix: &Array2<f32>) -> Prob {
    let nqueries = layout_matrix.rows();
    let mut probs = Prob::new(nqueries);
    let epsilon: f32 = 1.0 / nqueries as f32;

    // the index of the array is query id
    // get probs matrix
    if let Some(obj) = dist.as_object() {
        debug!("Model Parameters {:?}", obj);
        for (time, model) in obj {
            let time = time.parse::<i32>().unwrap();

            let xmu = model["xmu"].as_f64().unwrap();
            let ymu = model["ymu"].as_f64().unwrap();
            // does not take into account correlation
            let xsigma = model["xsigma"].as_f64().unwrap();
            let ysigma = model["ysigma"].as_f64().unwrap();

            // for each xpw, xmw, yph, ymh (col in layout matrix) compute cdf into cdf_array
            let col_xpw = layout_matrix.column(0);
            let col_xmw = layout_matrix.column(1);
            let col_yph = layout_matrix.column(2);
            let col_ymh = layout_matrix.column(3);

            let out_col_xpw = cdf_array(col_xpw, xmu, xsigma);
            let out_col_xmw = cdf_array(col_xmw, xmu, xsigma);
            let out_col_yph = cdf_array(col_yph, ymu, ysigma);
            let out_col_ymh = cdf_array(col_ymh, ymu, ysigma);
            let probs_t = &out_col_xpw * &out_col_yph
                - &out_col_xpw * &out_col_ymh
                - &out_col_xmw * &out_col_yph
                + &out_col_xmw * &out_col_ymh;

            let mut sub_queries_idx: Vec<usize> = Vec::new();
            let mut sub_qprobs: Vec<f32> = Vec::new();
            let mut max: f32 = 0.0;
            let mut max_index: i32 = -1;
            let mut sum_probs: f32 = 0.0;
            for (i, &item) in probs_t.iter().enumerate() {
                if item < epsilon {
                    continue;
                }
                sum_probs += item;
                sub_queries_idx.push(i);
                sub_qprobs.push(item);
                if item > max {
                    max = item;
                    max_index = i as i32;
                }
            }

            let mut diff: f32 = 0.0;
            if sum_probs < 1.0 {
                diff = 1.0 - sum_probs;
            }
            debug!(
                "sum probs: {:?} max: {} max_index: {}, diff {}",
                sum_probs, max, max_index, diff
            ); // it does equal to 1.0
            assert_eq!(
                sub_queries_idx.len(),
                sub_qprobs.len(),
                "sub_queries.len() {:?} != sub_qprobs.len() {:?}",
                sub_queries_idx.len(),
                sub_qprobs.len()
            );

            // get the ID with the maximum probability
            // subtract 1.0 - sum(prob)
            // give the rest to the ID with the highest probability
            let mut map: indexmap::IndexMap<usize, f32> = indexmap::IndexMap::new();
            // 5. construct the map
            for (i, qidx) in sub_queries_idx.iter().enumerate() {
                let mut p = sub_qprobs[i];
                if max_index == i as i32 {
                    p += diff;
                }
                map.insert(*qidx, p);
            }

            probs.set_probs_at(map, time as usize);
        }
    }

    probs
}

pub fn decode_lazy_model(dist: &serde_json::Value, nqueries: usize) -> LazyProb {
    let mut probs = LazyProb::new(nqueries);
    if let Some(obj) = dist.as_object() {
        for (time, model) in obj {
            let time = time.parse::<i32>().unwrap();
            let xmu = model["xmu"].as_f64().unwrap();
            let ymu = model["ymu"].as_f64().unwrap();
            // does not take into account correlation
            let xsigma = model["xsigma"].as_f64().unwrap();
            let ysigma = model["ysigma"].as_f64().unwrap();
            probs.set_probs_by_params(time as usize, xmu, ymu, xsigma, ysigma);
        }
    }
    probs
}
