/* ----------- Implementation for Gallery App -----------
 * @ client side:
 * // New Server: Register Application
 * let appstate = {"appname": "Gallery",
 *                  "cachesize": number,
 *                  "state": {"dbname": string, "factor": 10, "dimension": 600}}
 *
 * ------------------------------------------------------
 *
 * "dbname":     Database name.
 * "dimension":  Dimension of the gallary in pixels, default: 600 px.
 * "factor":     Number of columns and rows, grid=factor x factor
 * ------------------------------------------------------
 *
 * To run the scheduler, blocks per query and explicit queries need to be know before hand:
 *      @let blocks_per_query = backend.collect_blocks_per_query(GalleryApp::count_blocks);
 *      @let queries: Vec<String> = blocks_per_query.iter().map(|(k,_v)| k.to_string()).collect();
 *
 * If these are missing, Gaussian Model deocding wouldn't work.
 *
 * */

extern crate image;
use ndarray::Array2;
use serde_derive::{Deserialize, Serialize};
use std::io::prelude::*;

use super::layout;
use super::AppTrait;
use crate::backend;
use crate::ds;
use crate::scheduler;

#[derive(Clone)]
pub struct GalleryApp {
    backend: backend::inmem::InMemBackend,
    layout: layout::Layout,
    queries: Vec<String>,
    layout_matrix: Array2<f32>,
    blocks_per_query: indexmap::IndexMap<String, usize>,
    utility: Vec<f32>,
    blockcount: usize,
    config: serde_json::Value,
    blocksize: usize,
    max_blocks_count: usize,
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

// app specific
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub x: u32,
    pub y: u32,
}

pub fn new(appstate: &ds::AppState, config: serde_json::Value) -> GalleryApp {
    let backend: backend::inmem::InMemBackend;

    let (db_path, dimension, factor): (String, u32, u32) = {
        let mut dbname_out = "db_default_f10";
        let mut dimension = 600; // dimension in pixels
        let mut factor = 10; // how many imgs in a row/col
        if let Some(config) = appstate.state.as_object() {
            info!("config: {:?}", config);
            if let Some(out) = config.get("dbname") {
                dbname_out = out.as_str().unwrap_or(dbname_out);
            }

            if let Some(out) = config.get("dimension") {
                dimension = out.as_u64().unwrap_or(dimension as u64) as u32;
            }

            if let Some(out) = config.get("factor") {
                factor = out.as_u64().unwrap_or(factor as u64) as u32;
            }
        }

        (format!("data/{}", dbname_out), dimension, factor)
    };

    // move this to a new setup rust main function that only runs once
    if std::path::Path::new(&db_path).exists() == true {
        backend = backend::inmem::InMemBackend::new(db_path);
    } else {
        panic!("backend is not initialized {:?}", db_path);
    }

    let blocks_per_query = backend.collect_blocks_per_query(GalleryApp::count_blocks);

    // get block size
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

    info!("block size: {:?}", blocksize);

    let queries: Vec<String> = blocks_per_query
        .iter()
        .map(|(k, _v)| k.to_string())
        .collect();
    let layout = layout::Layout::new(dimension, factor);
    let layout_matrix: Array2<f32> = layout.get_layout(&queries);
    let max_blocks_count: usize = blocks_per_query
        .iter()
        .map(|(_, v)| *v)
        .max()
        .unwrap_or_else(|| 0);

    // todo: get this from client
    let utility = vec![
        0.5368018969864602,
        0.5989329283183455,
        0.6204741087175164,
        0.6384807893881198,
        0.6555823900049388,
        0.6722307700057626,
        0.6799509784873738,
        0.6974422244108067,
        0.7154107766582742,
        0.7262742653667537,
        0.7369252771584911,
        0.7476582768165855,
        0.7573153926906695,
        0.7675714529596727,
        0.7773649913422153,
        0.7878801558984999,
        0.7980835109041362,
        0.8085769234578454,
        0.8185072723223399,
        0.8278601350676332,
        0.8371821062289783,
        0.8395821797742525,
        0.8399697096446884,
        0.8414352093125254,
        0.8493688257276137,
        0.8560914102256155,
        0.8625998454743852,
        0.8694116579911336,
        0.875980651354279,
        0.8827054959438135,
        0.8893460503392506,
        0.895418894012518,
        0.9014408484204809,
        0.9075648627203988,
        0.913471098796932,
        0.919332955792312,
        0.9254747597736287,
        0.9317346907716734,
        0.9381627597953585,
        0.944175276333019,
        0.9507484812581765,
        0.957482143659201,
        0.9642912476746717,
        0.9709430753895002,
        0.9769001604477922,
        0.9827775653705169,
        0.9887195657917105,
        0.9945076138786211,
        0.9999128860173208,
        1.0,
    ];
    //let utility: Vec<f32> = (0..max_blocks_count).enumerate().map(|(i, _)| (1.0 / max_blocks_count as f32)*(i as f32+1.0) ).collect();

    debug!("utility: {:?} {:?}", utility.len(), utility);

    let blockcount: usize = match config["blockcount"].as_u64() {
        Some(b) => b as usize,
        None => 0, // use all blocks by default
    };

    GalleryApp {
        config: config,
        blockcount: blockcount,
        backend: backend,
        layout: layout,
        queries: queries,
        layout_matrix: layout_matrix,
        blocks_per_query: blocks_per_query,
        utility: utility,
        blocksize,
        max_blocks_count: max_blocks_count,
    }
}

impl GalleryApp {
    /// This creates an image gallery of one data file
    /// similar to the one we used for the paper submission
    /// preprocess image gallary and create/store blocks in db
    /// for ease of querying later
    pub fn setup(
        db_path: &str,
        img_file: &str,
        blocksize: usize,
        factor: u32,
    ) -> backend::inmem::InMemBackend {
        // initialize backend db
        let mut backend = backend::inmem::InMemBackend::new(db_path.to_string());

        // process images and create/store blocks in db to query them at run time
        let blocks = GalleryApp::create_blocks(img_file.to_owned(), blocksize);
        // serialize them to store them as binary in the backend
        let bytes = bincode::serialize(&blocks).unwrap();
        for x in 0..factor {
            for y in 0..factor {
                let q = Query { x: x, y: y };
                let key = GalleryApp::encode_key(&q);
                println!("{}, {}, {:?}", x, y, key);
                debug!("{}, {}, {:?}", x, y, key);
                backend.set(key, bytes.clone());
            }
        }

        backend.flush();

        backend
    }

    /// For demo, this create Gallery of different images
    /// Good for debugging, and observing the responsivness of the system.
    /// Assumption: folder "data/gallary/5/y/x.jpg" exists.
    pub fn setup_all(
        db_path: &str,
        inputfolder: &str,
        blocksize: usize,
        factor: u32,
    ) -> backend::inmem::InMemBackend {
        // initialize backend db
        let mut backend = backend::inmem::InMemBackend::new(db_path.to_string());

        // process images and create/store blocks in db to query them at run time
        // serialize them to store them as binary in the backend

        for x in 0..factor {
            for y in 0..factor {
                let fname = format!("{}/{}/{}/{}.jpg", inputfolder, factor, y, x);
                if std::path::Path::new(&fname).exists() == false {
                    panic!("Image not found {:?}", fname);
                }
                println!("fname: {:?}", fname);
                let blocks = GalleryApp::create_blocks(fname, blocksize);
                let bytes = bincode::serialize(&blocks).unwrap();
                let q = Query { x: x, y: y };
                let key = GalleryApp::encode_key(&q);
                println!("{}, {}, {:?}", x, y, key);
                debug!("{}, {}, {:?}", x, y, key);
                backend.set(key, bytes.clone());
            }
        }

        backend.flush();
        backend
    }

    fn count_blocks(v: &Vec<u8>) -> usize {
        let value: Vec<ImageBlock> = bincode::deserialize(&v).unwrap();
        let blocks_count = value.len();

        blocks_count
    }

    pub fn encode_key(q: &Query) -> Vec<u8> {
        let key = serde_json::to_string(&q).unwrap();
        //let key = bincode::serialize(&q).unwrap();

        key.as_bytes().to_vec()
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

    fn get_fake_block_bytes(
        &self,
        key: &str,
        _count: usize,
        incache: usize,
    ) -> Option<Vec<ds::StreamBlock>> {
        if incache > self.max_blocks_count {
            None
        } else {
            let mut sblocks: Vec<ds::StreamBlock> = Vec::new();
            let nblocks: u32 = self.max_blocks_count as u32;
            let mut bytebuffer: Vec<u8> = Vec::new();

            let blocksize = self.blocksize;
            let fake_block = ImageBlock {
                block_id: incache as u32,
                content: vec![0; blocksize],
            };
            let mut block_byte = fake_block.serialize();

            let size: u32 = block_byte.len() as u32;
            let blockid: u32 = incache as u32;
            let mut block_id = bincode::serialize(&blockid).unwrap();
            let mut nblock = bincode::serialize(&nblocks).unwrap();
            let mut key_byte = bincode::serialize(&key).unwrap();

            bytebuffer.append(&mut block_id);
            bytebuffer.append(&mut nblock);
            bytebuffer.append(&mut key_byte);
            bytebuffer.append(&mut block_byte);
            info!("fake ImageBlock {}, incache: {} nblocks: {:?} block#: {:?} size: {:?}, blocksize: {:?}",  key, incache,nblocks, incache, bytebuffer.len(), size);

            sblocks.push(ds::StreamBlock::Binary(bytebuffer));

            Some(sblocks)
        }
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
                if self.blockcount > 0 && sblocks.len() >= self.blockcount {
                    // progressive: nblocks = 1
                    break;
                }

                let block = &blocks[i];
                let mut bytebuffer: Vec<u8> = Vec::new();
                // start ring cache that checks if the client cache has already got rid of this
                // block, should we send this block or a new one?
                // for key and count and decision, construct the message to stream to the client

                let mut block_byte = block.serialize();
                let size: u32 = block_byte.len() as u32;
                let mut block_id = bincode::serialize(&block.block_id).unwrap();
                let mut nblock = bincode::serialize(&nblocks).unwrap();
                let mut key_byte = bincode::serialize(&key).unwrap();

                bytebuffer.append(&mut block_id);
                bytebuffer.append(&mut nblock);
                bytebuffer.append(&mut key_byte);
                bytebuffer.append(&mut block_byte);
                info!("ImageBlock i {} {}, incache: {} nblocks: {:?} block#: {:?} size: {:?}, blocksize: {:?}",  i, key, incache,nblocks, block.block_id, bytebuffer.len(), size);

                sblocks.push(ds::StreamBlock::Binary(bytebuffer));
            }

            Some(sblocks)
        } else {
            None
        }
    }
}

impl AppTrait for GalleryApp {
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
        match kv {
            Some((k, _)) => {
                //match self.config["use_netem"].as_bool() {
                if self.config["use_mahimahi"].as_bool() == Some(true)
                    || self.config["use_netem"].as_bool() == Some(true)
                {
                    self.get_fake_block_bytes(k, count, incache)
                } else {
                    self.get_nblocks_bytes(k, count, incache)
                }
            }
            None => None,
        }
    }

    fn get_nblocks_bykey(
        &mut self,
        key: &str,
        count: usize,
        incache: usize,
    ) -> Option<Vec<ds::StreamBlock>> {
        self.get_nblocks_bytes(key, count, incache)
    }

    fn decode_dist(&mut self, userstate: ds::PredictorState) -> scheduler::Prob {
        self.layout
            .decode_dist(userstate, &self.layout_matrix, &self.blocks_per_query)
    }

    fn get_block_size(&self) -> usize {
        self.blocksize
    }

    fn shutdown(&mut self) {
        debug!("shutting down");
        self.backend.flush();
        self.backend.drop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // cargo test test_gallery_preprocess -- --nocapture
    #[test]
    fn test_gallery_preprocess() {
        let factor: u32 = 10;
        let block_size = 20 * 1024;
        let db_path = "data/db_default_f10";
        let img_file = "data/img_5_30_11.jpg";
        // this will create gallery with the same image to create
        // a gallery with different images comment the following line
        // GalleryApp::setup(&db_path, &img_file, block_size, factor);
        // and uncomment the following two lines, update inputfolder to point
        // to the source of the gallery images
        let inputfolder = "data/progressive_f100";
        GalleryApp::setup_all(&db_path, &inputfolder, block_size, factor);
    }
}
