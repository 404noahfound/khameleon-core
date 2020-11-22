mod apps;
mod backend;
/// local imports
mod ds;
mod manager;
mod scheduler;
mod webserver;

extern crate crossbeam;
extern crate crossbeam_utils;
extern crate csv;
/// public libs
extern crate lp_modeler;

#[macro_use]
extern crate indexmap;

#[macro_use]
extern crate actix_web;

#[macro_use]
extern crate log;
extern crate fern;

extern crate rand;

use fern::colors::{Color, ColoredLevelConfig};

extern crate chrono;

#[macro_use]
extern crate ndarray;

use serde_json::json;

use actix::prelude::*;
use actix_session::CookieSession;
use actix_web::{middleware, App, HttpServer};

fn main() -> std::io::Result<()> {
    // setup logging environment
    // 1) create `log` directory if it doesnt exist
    std::fs::create_dir_all("./log/")?;

    std::env::set_var("RUST_LOG", "actix_web=info");
    let colors_line = ColoredLevelConfig::new()
        .error(Color::Red)
        .warn(Color::Yellow)
        // we actually don't need to specify the color for debug and info, they are white by default
        .info(Color::Green)
        .debug(Color::Magenta)
        // depending on the terminals color scheme, this is the same as the background color
        .trace(Color::Cyan);

    // configure colors for the name of the level.
    // since almost all of them are the some as the color for the whole line, we
    // just clone `colors_line` and overwrite our changes
    let colors_level = colors_line.clone();
    let log_level = log::LevelFilter::Debug;
    //let log_level = log::LevelFilter::Info;

    fern::Dispatch::new()
        // Perform allocation-free log formatting
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}]@({:?}) {}",
                chrono::Local::now().format("[%m-%d][%H:%M:%S]"),
                record.target(),
                colors_level.color(record.level()),
                match record.line() {
                    Some(line) => line,
                    None => 0,
                },
                message
            ))
        })
        .chain(
            // write to stdout
            fern::Dispatch::new()
                .level(log_level)
                .level_for("tokio_reactor", log::LevelFilter::Error)
                .chain(std::io::stdout()),
        )
        .chain(
            // write to file
            fern::Dispatch::new()
                .level(log_level)
                .level_for("tokio_reactor", log::LevelFilter::Error)
                //.filter(|metadata| {
                //   metadata.target() == "khameleon::manager" || metadata.target() == "khameleon::apps::gallary" || metadata.target() == "khameleon::webserver::ws"
                // })
                .chain(fern::log_file("log/actix.log")?),
        )
        // Apply globally
        .apply()
        .unwrap();

    // Read command line arguments: config file name
    let args: Vec<String> = std::env::args().collect();
    debug!("command line arguments: {:?}", args);
    let config: serde_json::Value = match args.len() > 1 {
        true => {
            // pass this as argument
            let fname = &args[1];
            let file = std::fs::File::open(fname).expect("file should open read only");
            let config: serde_json::Value =
                serde_json::from_reader(file).expect("JSON was not well-formatted");
            debug!("config: {:?} {:?}", fname, config);
            config
        }
        false => json!({}), // empty config file
    };

    info!("start server");
    let sys = actix_rt::System::new("khameleon-actix");

    // 2) Start Manager Thread/Actor
    let imanager = manager::Manager::new(config);
    let manager_addr = imanager.start();

    // 3) Initialize &start server and websocket
    HttpServer::new(move || {
        App::new()
            .data(manager_addr.clone())
            .configure(webserver::appconfig::config_app)
            // enable logger
            .wrap(middleware::Logger::default())
            .wrap(CookieSession::signed(&[0; 32]).secure(false))
    })
    .bind("0.0.0.0:8080")?
    .start();
    sys.run()
}
