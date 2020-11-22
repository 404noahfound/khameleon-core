/// local imports
use crate::ds;
use crate::manager;

use actix_web::{web, Error, HttpRequest, HttpResponse, Result};
use actix_web_actors::ws;
use crossbeam_utils::atomic::AtomicCell;
use csv::Writer;
/// public lib
use serde_derive::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
// for the Actor primitive
use actix::prelude::*;

#[derive(Serialize)]
struct BlockDelays {
    bid: u32,
    delay: u128,
    t1: u128,
    t2: u128,
    client: u128,
}

impl Handler<ds::StreamBlock> for WebSocket {
    type Result = ();

    fn handle(&mut self, block: ds::StreamBlock, ctx: &mut Self::Context) {
        match block {
            ds::StreamBlock::Binary(x) => {
                let mut bytebuffer: Vec<u8> = Vec::new();

                // metadata attached to each block to help track their rrt
                let bid: u32 = {
                    let timestamp: u128 = {
                        let now = std::time::SystemTime::now();
                        let since_the_epoch = now
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Time went backwards");
                        since_the_epoch.as_millis() as u128
                    };

                    self.block_counter += 1;

                    self.last_timestamp = timestamp;
                    self.blocks_tracker.insert(self.block_counter, timestamp);
                    self.block_counter
                };
                let mut bid = bincode::serialize(&bid).unwrap();

                bytebuffer.append(&mut bid);
                bytebuffer.extend(x);
                ctx.binary(bytebuffer)
            }
            ds::StreamBlock::Stop => ctx.stop(),
        }
    }
}

pub struct WebSocket {
    /// Stream Server address
    pub addr: Addr<manager::Manager>,
    pub block_counter: u32,
    pub blocks_tracker: HashMap<u32, u128>,
    pub writer: Writer<std::fs::File>,
    pub congestion: Arc<AtomicCell<u128>>,
    pub last_timestamp: u128,
}

impl Actor for WebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("Initializing WebSocket Actor");
        let addr = ctx.address();
        self.addr
            .send(manager::Connect {
                ws_addr: addr.recipient(),
                congestion: self.congestion.clone(),
            })
            .into_actor(self)
            .then(|res, _, ctx| {
                // pass on the laten
                match res {
                    Ok(_) => info!("successfully initialized ws"),
                    // something is wrong with server
                    _ => {
                        ctx.stop();
                        panic!("websocket initialization error");
                    }
                }
                fut::ok(())
            })
            .wait(ctx);
    }
}

// handler for 'ws::Message'
impl StreamHandler<ws::Message, ws::ProtocolError> for WebSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        // process websocket messages
        match msg {
            ws::Message::Ping(msg) => {
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => (),
            ws::Message::Text(text) => {
                let lines = text.split_whitespace();
                let nums: Vec<&str> = lines.collect();
                let bid = nums[0];
                let client_timestamp = {
                    match nums.len() == 2 {
                        true => nums[1].parse::<u128>().unwrap(),
                        false => 0,
                    }
                };

                match bid.parse::<u32>() {
                    Ok(n) => match self.blocks_tracker.get(&n) {
                        Some(&t1) => {
                            let t2: u128 = {
                                let now = std::time::SystemTime::now();
                                let since_the_epoch = now
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .expect("Time went backwards");
                                since_the_epoch.as_millis() as u128
                            };

                            let delay = t2 - t1;
                            self.congestion.store(delay);
                            match self.writer.serialize(BlockDelays {
                                bid: n,
                                delay: delay,
                                t1: t1,
                                t2: t2,
                                client: client_timestamp,
                            }) {
                                Ok(_) => (),
                                Err(e) => println!("writing to writer: {:?}", e),
                            }
                            let _ = self.writer.flush();
                        }
                        None => error!("no matching timestamp in blocks tracker {:?}", n),
                    },
                    Err(_) => error!("something wrong with the received block index {:?}", bid),
                }
            }
            ws::Message::Binary(bin) => {
                info!("Received bin: {:?}", bin);
            }
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }
}

pub fn ws_index(
    srv: web::Data<Addr<manager::Manager>>,
    r: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    info!("Initialize websocket header: {:?}", r);

    let fname = format!("./log/block_details.csv");
    let wtr = Writer::from_path(fname).unwrap();
    let congestion = Arc::new(AtomicCell::new(0));
    let websocket = WebSocket {
        addr: srv.get_ref().clone(),
        block_counter: 0,
        blocks_tracker: HashMap::new(),
        writer: wtr,
        congestion: congestion,
        last_timestamp: 0,
    };
    let res = ws::start(websocket, &r, stream);

    info!("ws session header response: {:?}", res.as_ref().unwrap());
    res
}
