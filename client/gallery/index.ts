import {
  Engine, KalmanFilter,
  ContinuesLogger
} from "../khameleon-core";
import { Gallery } from "../apps";

const DEFAULT_APP_CONFIG = {
  dbname: "db_default_f10",
  factor: 1000,
  tile_dimension: 10000,

  request: 0, // direct request
  progressive: 1, // to adjust cache size

  cachesize: 10000,
  cachetype: "ringIndex",
  duration: 1000 * 30, // 30 seconds
  // logTrace: true,
  // load_trace_mode: false,
  load_trace_mode: true,
};

function instance(opt?) {
  const config = { ...DEFAULT_APP_CONFIG, ...(opt || {}) };

  let logger = new ContinuesLogger();
  let predictor = new KalmanFilter(logger);

  config.cacheConfig = {
    cache: config.cachetype,
    cacheSize: config.cachesize,
  };

  const vizApp = new Gallery(config, logger);
  const engine = new Engine(vizApp, predictor, config);
  engine.run();

  return { engine: engine, vizApp: vizApp, predictor };
}


let { predictor } = instance();
predictor.logger.start(DEFAULT_APP_CONFIG.load_trace_mode);
