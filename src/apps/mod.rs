use serde_derive::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

// Available Apps
pub mod gallery;
pub mod testapp;

use crate::ds;
use crate::scheduler;

/// AppType: an enum that has the different types of apps supported
///          to add a new app, add here name of the app, and in the
///          new function below, include match for the new type
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum AppType {
    TestApp,
    Gallery,
}

/// apps::new: function used by the manager to create app instance
///            app struct has to support AppTrait trait
///            an example of an app implementation is in gallary.ds file
pub fn new(
    appstate: &ds::AppState,
    config: serde_json::Value,
    _state_change_flag: Arc<RwLock<bool>>,
) -> Box<dyn AppTrait> {
    match appstate.appname {
        AppType::TestApp => Box::new(testapp::new(appstate, config)) as Box<dyn AppTrait>,
        AppType::Gallery => Box::new(gallery::new(appstate, config)) as Box<dyn AppTrait>,
    }
}

/// AppTrait: apps need to supprt this trait, it recieves distrubtion from client
///           and run scheduler  to decide  list of blocks to stream using 'get_decisions',
///           and the actual blocks as a vector of blocks to stream to the client using
///           'stream_blocks'

pub trait AppTrait: Send + Sync {
    /// Returns data needed by the scheduler:
    /// (1) blocks per query. We use 'indexmap' because each query is identified
    ///     by both a unique integer ID and a String key
    /// (2) utility function
    ///
    /// # Example
    /// let (blocks_per_query, utility) = app.get_scheduler_config();
    fn get_scheduler_config(&self) -> (indexmap::IndexMap<String, usize>, Vec<f32>);

    /// decode received distribution from the client and return information in Prob object
    fn decode_dist(&mut self, userstate: ds::PredictorState) -> scheduler::Prob;

    /// return size of a block in Bytes
    fn get_block_size(&self) -> usize;

    /// since scheduler uses assigned IDs to queries, this used to
    /// retrieves 'count' blocks for query with index='index'
    fn get_nblocks_byindex(
        &mut self,
        _index: usize,
        _count: usize,
        _incache: usize,
    ) -> Option<Vec<ds::StreamBlock>> {
        None
    }

    /// optional: Retrieves 'count' blocks for query with key='key'
    fn get_nblocks_bykey(
        &mut self,
        _key: &str,
        _count: usize,
        _incache: usize,
    ) -> Option<Vec<ds::StreamBlock>> {
        None
    }

    /// optional: cleanup before app closes
    fn shutdown(&mut self) {
        error!("received Ctrl+C!");
    }

    /// optional: data to initialize client's state
    fn get_initstate(&mut self) -> String {
        "".to_owned()
    }

    /// optional: app specific policies to modify sequence of blocks
    fn prepare_schedule(&mut self, _schedule: &Vec<usize>) {}
}
