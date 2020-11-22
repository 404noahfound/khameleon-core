use crate::manager;

use actix::prelude::*;
use actix_files as fs;
use actix_rt::spawn;
use actix_session::Session;
use actix_web::http::StatusCode;
use actix_web::{error, web, Error, FromRequest, HttpRequest, HttpResponse, Result};
use futures::{future::ok as fut_ok, Future};
use serde_derive::{Deserialize, Serialize};

/// serve multi_index.html
#[get("/")]
fn index(session: Session, req: HttpRequest) -> Result<HttpResponse> {
    debug!("{:?}", req);

    let mut counter = 1;
    if let Some(count) = session.get::<i32>("counter")? {
        info!("Session value: {}", count);
        counter = count + 1;
    }

    // set counter to sesssion
    session.set("counter", counter)?;
    // response
    Ok(HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../client/main/index.html")))
}

pub fn log_bandwidth_handle(
    srv: web::Data<Addr<manager::Manager>>,
    msg: String,
) -> impl Future<Item = String, Error = Error> {
    let stat: manager::SystemStat = serde_json::from_str(&msg).unwrap();
    let actor_req = srv.send(stat);
    actor_req
        .map_err(error::Error::from)
        .and_then(|_| fut_ok("done".to_owned()))
}

pub fn start_threads_handle(
    srv: web::Data<Addr<manager::Manager>>,
) -> impl Future<Item = String, Error = Error> {
    let actor_req = srv.send(manager::manager::StartThreads);
    actor_req
        .map_err(error::Error::from)
        .and_then(|_| fut_ok("done".to_owned()))
}

pub fn log_latency_handle() -> Result<()> {
    Ok(())
}

pub fn direct_request(
    srv: web::Data<Addr<manager::Manager>>,
    msg: String,
) -> impl Future<Item = String, Error = Error> {
    let request: manager::Request = match serde_json::from_str(&msg) {
        Ok(content) => content,
        Err(err) => panic!("direct_request msg({:?}) error ({:?})", msg, err),
    };
    let actor_req = srv.send(request);

    actor_req.map_err(error::Error::from).and_then(|_| {
        // get feedback from the app and pass it to the client
        fut_ok("done".to_owned())
    })
}

// todo: add a handler to handle layout updates
pub fn init_app_handle(
    srv: web::Data<Addr<manager::Manager>>,
    msg: String,
) -> impl Future<Item = String, Error = Error> {
    // takes on msg as String and use Value to deserialize it
    let actor_req = srv.send(manager::InitApp { state: msg });
    actor_req.map_err(error::Error::from).and_then(|data| {
        info!("init app state {}", data.instance);

        if data.instance > 1 {
            error!("one insrtance is already running: {}", data.instance);
            // todo: find a way to clear and start without
            // killing the app
            System::current().stop();
            panic!("one instance is already running");
        } else {
            // get feedback from the app and pass it to the client
            fut_ok(data.data)
        }
    })
}

/// https://docs.serde.rs/serde_json/enum.Value.html
pub fn distribution_handle(srv: web::Data<Addr<manager::Manager>>, msg: String) -> Result<()> {
    // takes on msg as String and use Value to deserialize it
    let res = srv.send(manager::Distributions { data: msg });
    spawn(res.map(|_| ()).map_err(|_| ()));

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct TraceData {
    data: serde_json::Value,
    file: String,
}

pub fn logtrace_tofile_handle(msg: String) -> Result<HttpResponse> {
    let datainfo: TraceData = match serde_json::from_str(&msg) {
        Ok(content) => content,
        Err(err) => panic!("logtrace_tofile_handle msg({:?}) error ({:?})", msg, err),
    };

    let path = "./traces/";

    // create dir if it doesnt exist
    std::fs::create_dir_all(&path)?;

    let path = format!("{}/{}", path, datainfo.file);

    info!("log trace into: {:?}", path);

    let path = std::path::Path::new(&path);
    let display = path.display();

    match std::fs::File::create(path) {
        Ok(file) => {
            serde_json::to_writer_pretty(file, &datainfo.data)?;
            Ok(HttpResponse::Ok().finish())
        }
        Err(why) => {
            error!("couldnt open {} {}", display, why);
            Ok(HttpResponse::InternalServerError()
                .body(format!("couldn't open {} {}", display, why)))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ResultsData {
    data: serde_json::Value,
}

pub fn log_tofile_handle(msg: String) -> Result<HttpResponse> {
    let datainfo: ResultsData = match serde_json::from_str(&msg) {
        Ok(content) => content,
        Err(err) => panic!("log_tofile_handle msg({:?}) error ({:?})", msg, err),
    };

    let path = "./log/exp_details.json";
    info!("log_results {:?}", path);
    let path = std::path::Path::new(&path);
    let display = path.display();

    match std::fs::File::create(path) {
        Ok(file) => {
            serde_json::to_writer_pretty(file, &datainfo.data)?;
            Ok(HttpResponse::Ok().finish())
        }
        Err(why) => {
            error!("couldnt open {} {}", display, why);
            Ok(HttpResponse::InternalServerError()
                .body(format!("couldn't open {} {}", display, why)))
        }
    }
}

/// initialize handles
pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/log/write")
            .data(String::configure(|g| g.limit(1024 * 1024 * 100)))
            .route(web::post().to(log_tofile_handle)),
    )
    .service(
        web::resource("/log/trace")
            .data(String::configure(|g| g.limit(1024 * 1024 * 100)))
            .route(web::post().to(logtrace_tofile_handle)),
    )
    .service(web::resource("/request").route(web::post().to_async(direct_request)))
    .service(index)
    .service(web::resource("/post_dist").route(web::post().to(distribution_handle)))
    .service(
        web::resource("/initapp")
            .data(String::configure(|g| g.limit(1024 * 1024 * 100)))
            .route(web::post().to_async(init_app_handle)),
    )
    .service(web::resource("/log/latency").route(web::post().to(log_latency_handle)))
    .service(web::resource("/log/bandwidth").route(web::post().to_async(log_bandwidth_handle)))
    .service(web::resource("/start/threads").route(web::post().to_async(start_threads_handle)))
    .service(web::resource("/ws/").route(web::get().to(super::ws::ws_index)))
    .service(fs::Files::new("static", "client/static").show_files_listing());
}
