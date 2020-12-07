import { App, Engine, Data, SystemLogger } from "../../khameleon-core";
import trace from "../../static/data/trace/1M-30s.json";
import * as d3 from "d3";
import * as _ from 'underscore';

interface RenderData {
  img_dir: any;
};

interface Query {
  x: number;
  y: number;
}

export class Gallery implements App {
  private engine: Engine;
  private factor: number;
  private move: boolean = false;
  private image_holder_dimension: number;
  private tile_dimension: number;
  private path: string;
  private prevData: Query | undefined = undefined;
  public appName: string = "Gallery";
  private dbname: string;
  private load_trace_mode: boolean;

  constructor(private sysconfig, private logger) {
    this.factor = (sysconfig && sysconfig.factor) ? sysconfig.factor : 10;
    this.image_holder_dimension = (sysconfig && sysconfig.image_holder_dimension) ? sysconfig.image_holder_dimension : 800;
    this.tile_dimension = (sysconfig && sysconfig.tile_dimension) ? sysconfig.tile_dimension : 600;
    this.path = (sysconfig && sysconfig.path) ? sysconfig.path : "static/data/";
    this.load_trace_mode = (sysconfig && sysconfig.load_trace_mode) ? sysconfig.load_trace_mode : false;

    this.dbname = (sysconfig && sysconfig.dbname) ? sysconfig.dbname : "db_default_f10";
  }

  bindEngine(engine: Engine) {
    this.engine = engine;
  }

  getState() {
    let appstate = {
      "dbname": this.dbname,
      "factor": this.factor,
      "dimension": this.tile_dimension
    };

    let state = {
      "appname": this.appName,
      "cachesize": this.sysconfig.cachesize,
      "state": appstate
    };

    return state;
  }

  decode_key(key: string): string {
    return key;
  }

  onopen(data: string) {
    console.log("Gallery data received", data);
    this.setup();
    console.log("start logger && predictor");
    this.engine.predictor.start();
    window.gsyslogger = new SystemLogger();
  }

  sendQuery(data: Query) {
    // the query is same as previous one?
    if (this.prevData && JSON.stringify(this.prevData) === JSON.stringify(data)) {
      return;
    }
    this.prevData = data;
    this.engine.registerQuery(JSON.stringify(data), this.render.bind(this));
  }

  setup() {
    let dim = this.image_holder_dimension;
    let tile_dim = this.tile_dimension;
    let offset = 50;
    let large_view_svg = d3.select("body").append("div")
      .style("left", tile_dim + offset + "px")
      .style("top", "0px")
      .style("position", "absolute")
      .append("svg");

    /* Main View */
    large_view_svg
      .attr("id", "large_view")
      .style("border", "2px solid black")
      .attr("width", dim)
      .attr("height", dim);

    large_view_svg.append("image")
      .attr("href", (_) => {
        return this.path + "/image_holder.jpg";
      })
      .attr("id", "large")
      .attr("x", 0)
      .attr("y", 0)
      .attr("width", dim)
      .attr("height", dim)
      .attr("preserveAspectRatio", "xMidYMin slice");

    d3.select("body").append("div")
      .style("left", 0)
      .style("bottom", 0)
      .style("position", "fixed")
      .attr("id", "utility")
      .style("width", tile_dim + "px")
      .style("height", "100px")
      .style("border", "3px solid #73AD21")
      .style("z-index", "1000")
      .style("font-size", "100px")
      .text("Utility X");

    const small_view_svg = d3.select("body").append("div")
      .style("left", "0px")
      .style("top", "0px")
      .style("width", tile_dim + "px")
      .style("height", tile_dim + "px")
      .style("border", "2px solid black")
      .style("position", "absolute")
      .append("svg");

    /* Main View */
    small_view_svg
      .attr("id", "nav_map")
      .attr("width", tile_dim)
      .attr("height", tile_dim);

    let that = this;
    small_view_svg.append("image")
      .attr("href", (_) => {
        return this.path + "tile.jpg";
      })
      .attr("id", "small")
      .attr("x", 0)
      .attr("y", 0)
      .attr("width", tile_dim)
      .attr("height", tile_dim)
      .attr("preserveAspectRatio", "xMidYMin slice")
      /* Draggable viewport */
      .on("mousemove", function () {
        if (that.load_trace_mode || !that.move) {
          return;
        }

        const x = that.logger.last_point[0];
        const y = that.logger.last_point[1];

        let query = that.getQueryByPosition(x, y);
        if (query) {
          that.sendQuery(query);
        }
      })
      .on("click", () => {
        this.move = !this.move
        if (this.load_trace_mode) {
          this.load_and_run_trace()
        }
      });
  }

  delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async load_and_run_trace() {
    this.logger.trace = [];
    let last_timestamp: any = trace[0].e[2];
    for (let trace_entry of trace) {
      let new_timestamp: any = trace_entry.e[2];
      let pause_interval: any = new_timestamp - last_timestamp;
      // while (Date.now() < last_time + pause_interval) { console.log(Date.now()) }
      await this.delay(pause_interval * 100);
      // await this.delay(1000);
      last_timestamp = new_timestamp;
      // recomputedTime += pause_interval;
      trace_entry.e[2] = Date.now();
      // console.log(trace_entry, Date.now());
      let x = trace_entry.e[0];
      let y = trace_entry.e[1];
      this.logger.last_point = trace_entry.e;
      this.logger.updateTrace(trace_entry.e);
      this.logger.formalize();
      let query = this.getQueryByPosition(x, y);
      if (query) {
        this.sendQuery(query);
      }
      // console.log(pause_interval);
    }
    console.log("trace is");
    console.log(this.logger.trace);
    location.reload(); // reload page to stop sending query
  }


  getQueryByPosition(x, y): Query | undefined {
    const tile_dim = this.tile_dimension / this.factor;
    const xy_position = GetMinMaxTilesCoord(x, y, tile_dim);
    const qmax = this.factor;
    const y_img = xy_position.y;
    const x_img = xy_position.x;

    function GetMinMaxTilesCoord(x, y, tile_dim) {
      const x_min = Math.floor((x / tile_dim));
      const y_min = Math.floor((y / tile_dim));
      let x_ = x_min;
      let y_ = y_min;
      return { x: x_, y: y_ };
    }

    if (x_img >= qmax || y_img >= qmax || x_img < 0 || y_img < 0) {
      return undefined;
    }
    return { "x": x_img, "y": y_img };
  }

  render(data: RenderData) {
    let dim = this.image_holder_dimension;

    d3.select("#large_view")
      .attr("width", dim)
      .attr("height", dim)
      .select("image")
      .attr("width", dim)
      .attr("height", dim)
      .attr("href", function (_) {
        return data.img_dir;
      })
      .on("load", function () {
        URL.revokeObjectURL(data.img_dir);
      });
  }

  decodeBlock(block: any) {
    let offset = 0;
    let block_id = new Uint32Array(block, 0, 1)[0]; offset += 4;  // u32
    let content_len = new Uint32Array(block, offset, 1)[0]; offset += 8; // u64
    let content = new Uint8Array(block, offset, content_len);

    //console.log("storeData: ", key, block_id, nblocks);
    let decodedblock = { "block_id": block_id, "content": content };
    return decodedblock;

    return undefined;
  }

  construct(req, blocks, nblocks): Data {
    let image_data: any[] = [];
    for (var i = 0; i < blocks.size; i++) {
      if (blocks.has(i)) {
        let { data } = blocks.get(i);
        let block = this.decodeBlock(data);
        if (block == undefined) break;
        image_data.push(new Uint8Array(block.content));
      } else {
        break;
      }
    }

    let img_dir = URL.createObjectURL(new Blob(image_data));

    d3.select("#utility")
      .text(req + " has " + image_data.length + " blocks out of " + nblocks);

    return { render_data: { img_dir: img_dir }, inblocks: image_data.length };
  }
}
