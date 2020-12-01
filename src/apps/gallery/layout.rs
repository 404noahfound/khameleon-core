use super::gallery;
use crate::ds;
use crate::scheduler;
use ndarray::{arr1, Array2};

#[derive(Debug, Clone)]
pub struct Layout {
    pub dim: u32,
    pub tile_dim: f32,
    pub factor: u32,
}

impl Layout {
    pub fn new(dim: u32, factor: u32) -> Layout {
        //let factor: u32 = 2 << zoom -1;
        let tile_dim: f32 = dim as f32 / factor as f32;
        Layout {
            dim: dim,
            factor: factor,
            tile_dim: tile_dim,
        }
    }

    pub fn pixel_to_query(&self, x: f64, y: f64) -> gallery::Query {
        let qx = (x / self.tile_dim as f64).floor() as u32;
        let qy = (y / self.tile_dim as f64).floor() as u32;

        gallery::Query { x: qx, y: qy }
    }

    pub fn get_layout(&self, queries: &Vec<String>) -> Array2<f32> {
        let nqueries = queries.len();
        let mut layout_matrix: Array2<f32> = Array2::ones((nqueries, 4));
        for (query_index, q) in queries.iter().enumerate() {
            let query: gallery::Query = serde_json::from_str(q).unwrap();

            let x_min = query.x as f32 * self.tile_dim;
            let y_min = query.y as f32 * self.tile_dim;
            let x_max = (query.x + 1) as f32 * self.tile_dim;
            let y_max = (query.y + 1) as f32 * self.tile_dim;

            //layout_matrix.column
            layout_matrix
                .slice_mut(s![query_index.., ..])
                .assign(&arr1(&[x_min, x_max, y_min, y_max]));
        }

        layout_matrix
    }

    pub fn decode_dist(
        &self,
        userstate: ds::PredictorState,
        layout_matrix: &Array2<f32>,
        queries_blcount: &indexmap::IndexMap<String, usize>,
    ) -> impl scheduler::ProbTrait {
        // what about applications that are hard to enumerate all possible queries before hand
        // debug!("userstate: {:?}", userstate);
        let decoded_dist = {
            match userstate.model.trim() {
                // "GM" => scheduler::decode_model(&userstate.data, layout_matrix),
                // "GM" => scheduler::decode_lazy_model(&userstate.data, layout_matrix.len()),
                "LGP" => {
                    match userstate.data.as_object() {
                        Some(obj) => {
                            let dist = obj["dist"].clone();
                            let dist: scheduler::decoders::LinearPointGaussian =
                                match serde_json::from_value(dist) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        error!("unexpected dist {:?}", e);
                                        panic!("unexpected dist {:?}", e);
                                    }
                                };
                            let (alpha, x, y) = scheduler::decode_point_model(&dist.p);
                            let qx = (x / self.tile_dim as f64).floor() as usize;
                            let qy = (y / self.tile_dim as f64).floor() as usize;
                            // let key = self.pixel_to_query(x, y);
                            // let key = serde_json::to_string(&key).unwrap();
                            // let index = {
                            //     match queries_blcount.get_full(&key) {
                            //         Some((idx, _, _)) => idx,
                            //         None => {
                            //             alpha = 1.0; // don't use point distribution
                            //             0 // any index
                            //         }
                            //     }
                            // };
                            let index = qx * self.factor as usize + qy;

                            // let mut prob = scheduler::decode_model(&dist.g, layout_matrix);
                            let mut prob = scheduler::decode_lazy_model(
                                &dist.g,
                                (self.factor * self.factor) as usize,
                                self.tile_dim as f64,
                            );
                            debug!("the prob model is {:?}", dist);
                            // debug!(
                            //     "qx is {:?}, qy is {:?}, factor is {:?}",
                            //     qx, qy, self.factor
                            // );
                            prob.set_point_dist(alpha, index);

                            // get the index of this query
                            // error!(
                            //     "alpha {:?}, x: {:?}, y: {:?} key: {:?} index: {:?} dist : {:?}",
                            //     alpha, x, y, key, index, dist
                            // );
                            prob
                        }
                        None => panic!("no match routine to decode this {}", userstate.model),
                    }
                }
                _ => panic!("no match routine to decode this {}", userstate.model),
            }
        };
        //debug!("decode_dist: {:?}", decoded_dist);

        decoded_dist
    }
}
