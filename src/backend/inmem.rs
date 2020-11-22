extern crate image;
use sled::Db;
extern crate base64;

#[derive(Clone)]
pub struct InMemBackend {
    dbname: String,
    db: sled::Db,
}

impl InMemBackend {
    pub fn new(dbname: String) -> Self {
        // initialize backend server
        let config = sled::ConfigBuilder::new().path(&dbname).build();

        InMemBackend {
            dbname: dbname,
            db: Db::start(config).unwrap(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        let _ = self.db.set(key, val);
    }

    pub fn drop(&mut self) {
        drop(&mut self.db);
    }

    // get values stored at key @key
    // if it doesnt exist return None+ warning?
    pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        if let Some(bytes) = self.db.get(&key).unwrap() {
            Some(bytes.to_vec())
        } else {
            error!("key {:?} is not in db", &key);
            None
        }
    }

    pub fn get_iter(&self) -> sled::Iter {
        self.db.iter()
    }

    pub fn flush(&mut self) {
        match self.db.flush() {
            Ok(result) => debug!("flushed successfully {:?}", result),
            Err(err) => error!("flush error: {:?}", err),
        };
    }

    pub fn collect_blocks_per_query(
        &self,
        f: fn(&Vec<u8>) -> usize,
    ) -> indexmap::IndexMap<String, usize> {
        let mut blocks_per_query: indexmap::IndexMap<String, usize> = indexmap::IndexMap::new();
        let iter = self.get_iter();
        for result in iter {
            match result {
                Ok((k, v)) => {
                    let blocks_count = f(&v.to_vec());
                    let key = std::str::from_utf8(&k).unwrap();
                    debug!("k: {:?} count: {:?}", key.to_string(), blocks_count);
                    blocks_per_query.insert(key.to_string(), blocks_count);
                }
                Err(err) => error!("{:?}", err),
            }
        }

        blocks_per_query
    }
}
