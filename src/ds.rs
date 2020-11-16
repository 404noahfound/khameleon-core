/* contains common data structs used between different modules
 *
 * TimeManager: stores system information that are needed to translate between server time
 *              to client time to enable model querying.
 */

/// local imports
use crate::apps;

/// for Message macro
use actix::prelude::*;
use crossbeam_utils::atomic::AtomicCell;
use serde_derive::{Deserialize, Serialize};
/// public lib
use serde_json::Value;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Message)]
pub enum StreamBlock {
    Binary(Vec<u8>),
    Stop,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredictorState {
    pub model: String,
    pub data: serde_json::Value,
}

impl PredictorState {
    pub fn new(model: &str, data: serde_json::Value) -> Self {
        PredictorState {
            model: model.to_owned(),
            data: data,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppState {
    pub appname: apps::AppType,
    pub cachesize: usize,

    // app specific initializations
    pub state: Value,
}

pub struct TimeManager {
    /// transfer time required for one block
    time_block_transfer_ms: usize,
    /// latency in ms
    latency: usize,
    bw: Arc<AtomicCell<f64>>,
    blocksize_megabits: f64,
    time: Option<std::time::Instant>,
}

impl TimeManager {
    pub fn new(time_block_transfer_ms: usize, latency: usize, bw: f64) -> Self {
        let bw = Arc::new(AtomicCell::new(bw));

        TimeManager {
            time_block_transfer_ms: time_block_transfer_ms,
            latency: latency,
            bw: bw,
            blocksize_megabits: 0.0,
            time: None,
        }
    }

    // Time it takes to send one block
    pub fn update_transfer_time(&mut self, bw: f64, blocksize_megabits: f64) {
        self.time_block_transfer_ms = ((blocksize_megabits / bw as f64) * 1000.0).ceil() as usize;
        debug!("time_block_transfer_ms: {:?}", self.time_block_transfer_ms);
    }

    /**
     * Time it takes for this slot to reach client
     */
    #[inline]
    pub fn slot_to_client_delta(&self, slot: usize) -> usize {
        let progress = match self.time {
            Some(time) => time.elapsed().as_millis() as usize,
            None => 0,
        };
        (self.latency / 2) + progress + slot * self.time_block_transfer_ms
    }

    pub fn update_blocksize_megabits(&mut self, bsize_megabits: f64) {
        self.blocksize_megabits = bsize_megabits;
        self.update_transfer_time(self.bw.load(), bsize_megabits);
    }
    pub fn update_latency(&mut self, latency: usize) {
        self.latency = latency;
    }

    pub fn update_bandwidth(&mut self, bw: f64) {
        self.bw.store(bw);
        self.update_transfer_time(bw, self.blocksize_megabits);
    }

    pub fn get_ref_bw(&self) -> Arc<AtomicCell<f64>> {
        self.bw.clone()
    }

    pub fn update_time(&mut self, time: std::time::Instant) {
        self.time = Some(time);
    }
}
