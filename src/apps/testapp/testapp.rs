use serde_derive::{Deserialize, Serialize};
use std::io::prelude::*;

use super::AppTrait;
use crate::backend;
use crate::ds;
use crate::scheduler;

#[derive(Clone)]
pub struct TestApp {
    blocks_per_query: indexmap::IndexMap<String, usize>,
    utility: Vec<f32>,
    blocksize: usize,
    backend: backend::inmem::InMemBackend,
}

/// appstate: specific data passed at initialization state from the client
/// config: configuration data passed from the server
pub fn new(_appstate: &ds::AppState, _config: serde_json::Value) -> TestApp {
    info!("1) load K/V store");
    let db_path = "data/test_data".to_string();
    let backend: backend::inmem::InMemBackend;
    if std::path::Path::new(&db_path).exists() == true {
        backend = backend::inmem::InMemBackend::new(db_path);
    } else {
        panic!("backend is not initialized {:?}", db_path);
    }

    info!("2) create an index  of how many blocks/query");
    let blocks_per_query = backend.collect_blocks_per_query(TestApp::count_blocks);
    let blocksize = match backend.get_iter().next() {
        Some(Ok((_k, v))) => {
            let value: Vec<ImageBlock> = bincode::deserialize(&v).unwrap();
            let size = match value.iter().next() {
                Some(v) => v.size(),
                None => 0,
            };

            size
        }
        _ => 0,
    };

    let max_blocks_count: usize = blocks_per_query
        .iter()
        .map(|(_, v)| *v)
        .max()
        .unwrap_or_else(|| 0);
    let utility: Vec<f32> = (0..max_blocks_count)
        .enumerate()
        .map(|(i, _)| (1.0 / max_blocks_count as f32) * (i as f32 + 1.0))
        .collect();
    TestApp {
        blocks_per_query,
        utility,
        blocksize,
        backend,
    }
}

// app specific
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ImageBlock {
    block_id: u32,
    content: Vec<u8>,
}

impl ImageBlock {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    fn size(&self) -> usize {
        self.content.len()
    }
}

impl TestApp {
    fn count_blocks(v: &Vec<u8>) -> usize {
        let value: Vec<ImageBlock> = bincode::deserialize(&v).unwrap();
        let blocks_count = value.len();

        blocks_count
    }

    fn create_blocks(fname: String, blocksize: usize) -> Vec<ImageBlock> {
        let mut file = std::fs::File::open(&fname).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        let img = buffer;

        let mut blocks = Vec::new();
        let mut start = 0;

        let blocksize = if blocksize > img.len() {
            img.len()
        } else {
            blocksize
        };
        let mut end = blocksize;

        debug!("blocksize: {:?} end: {:?}", blocksize, end);
        let mut bid = 0;

        while end <= img.len() {
            if end > img.len() {
                end = img.len();
            }

            blocks.push(ImageBlock {
                block_id: bid,
                content: img[start..end].to_vec(),
            });
            bid += 1;
            start = end;
            end += blocksize;
        }

        blocks
    }

    fn get_nblocks_bytes(
        &self,
        key: &str,
        count: usize,
        incache: usize,
    ) -> Option<Vec<ds::StreamBlock>> {
        if let Some(blocks_bytes) = self.backend.get(key.as_bytes().to_vec()) {
            let mut sblocks: Vec<ds::StreamBlock> = Vec::new();
            let blocks: Vec<ImageBlock> = bincode::deserialize(&blocks_bytes).unwrap();
            let nblocks: u32 = blocks.len() as u32;

            let end = if incache + count > blocks.len() {
                blocks.len()
            } else {
                incache + count
            };
            for i in incache..end {
                let block = &blocks[i];
                let mut bytebuffer: Vec<u8> = Vec::new();
                let mut block_byte = block.serialize();
                let mut block_id = bincode::serialize(&block.block_id).unwrap();
                let mut nblock = bincode::serialize(&nblocks).unwrap();
                let mut key_byte = bincode::serialize(&key).unwrap();

                bytebuffer.append(&mut block_id);
                bytebuffer.append(&mut nblock);
                bytebuffer.append(&mut key_byte);
                bytebuffer.append(&mut block_byte);

                sblocks.push(ds::StreamBlock::Binary(bytebuffer));
            }

            Some(sblocks)
        } else {
            None
        }
    }
}

impl AppTrait for TestApp {
    fn get_scheduler_config(&self) -> (indexmap::IndexMap<String, usize>, Vec<f32>) {
        (self.blocks_per_query.clone(), self.utility.clone())
    }

    fn get_nblocks_byindex(
        &mut self,
        index: usize,
        count: usize,
        incache: usize,
    ) -> Option<Vec<ds::StreamBlock>> {
        let kv = self.blocks_per_query.get_index(index);
        debug!("get {:?}", kv);
        match kv {
            Some((k, _)) => self.get_nblocks_bytes(k, count, incache),
            None => None,
        }
    }

    fn decode_dist(&mut self, userstate: ds::PredictorState) -> Box<dyn scheduler::ProbTrait> {
        debug!("decode_dist: {:?}", userstate);
        let total_queries = 1;
        let prob = scheduler::Prob::new(total_queries);
        Box::new(prob)
    }

    fn get_block_size(&self) -> usize {
        self.blocksize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // create kv store with single image data (key=R1) with blocks of size 20KB
    // $ cargo test test_testapp_prepreocess_backend -- --nocapture
    fn test_testapp_preprocess_backend() {
        let blocksize = 20 * 1024;
        let image_path = "data/img_5_30_11.jpg";
        let db_path = "data/test_data";
        let blocks = TestApp::create_blocks(image_path.to_string(), blocksize);
        // create backend key/value store
        let mut backend = backend::inmem::InMemBackend::new(db_path.to_string());
        let bytes = bincode::serialize(&blocks).unwrap();
        let query = "R1";
        let key = query.as_bytes().to_vec();
        backend.set(key, bytes.clone());
        backend.flush();
    }
}
