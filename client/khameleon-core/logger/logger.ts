// General continus Logger that logs mouse actions mouse{move}/{down}/{up}
// at `minResolution` interval, and maintains trace length of size `traceLength`
// TODO: what other ploicies an application might need?

import * as _ from 'underscore';

enum T {
    X = 0,
    Y = 1,
    time = 2,
    action = 3,
}

export class ContinuesLogger {
    private minResolution: number;
    private traceLength: number;
    private queuePolicy: string;
    // [x, y, time, action]
    private trace: Array<[number, number, number, string]>;
    private elem: Object;
    public last_point: [number, number, number, string] = [0, 0, 0, "0"];
    public last_timestamp: number = 0;

    // @opts logger configurations
    constructor(opts?) {
        opts = opts || {};
        this.minResolution = opts.minResolution || 20;
        this.traceLength = opts.traceLength || 30;
        this.queuePolicy = opts.queuePolicy || "FIFO";
        this.trace = [];
        this.elem = opts.elem || document;
    }

    start(load_trace_mode = true) {
        if (!load_trace_mode) {
            this.bind(this.elem);
        }
    }

    getTrace(): Array<[number, number, number, string]> | null {
        if (this.trace.length === 0)
            return null;
        const last = this.trace[this.trace.length - 1];
        const now = Date.now();
        if ((now - last[T.time]) > this.minResolution) {
            let point = [last[T.X], last[T.Y], now, "m"];
            this.padTrace(point, last);
        }
        //this.formalize();
        return this.trace;
    }

    _calcDist(x1, y1, x2, y2) {
        return Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
    }

    // modify the trace based on a queue policy
    formalize() {
        if (this.queuePolicy === "FIFO") {
            if (this.trace.length > this.traceLength) {
                const start = this.trace.length - this.traceLength;
                this.trace = this.trace.slice(start, this.trace.length);
            }
        }
    }

    // log x, y, time, action
    pushXYT(e, action) {
        const now = Date.now();
        this.addPoint([e.pageX, e.pageY, now, action]);

        this.formalize();
    }

    get_stopDelta() {
        if (window.session_config && window.session_config.macro_test) {
            return this.last_timestamp - this.last_point[2];
        }

        return Date.now() - this.last_point[2];
    }

    addPoint(point) {

        if (this.trace.length === 0) {
            this.updateTrace(point);
            return;
        }

        const first = this.trace[0];
        const last = this.trace[this.trace.length - 1];

        // truncate this.trace if we detect something crazy
        if (this.trace.length > 0) {
            const dist0 = this._calcDist(first[T.X], first[T.Y], last[T.X], last[T.Y]);
            const dist1 = this._calcDist(first[T.X], first[T.Y], point[T.X], point[T.Y]);
            if (dist0 > dist1) {
                this.trace = [last];
            }
        }

        this.padTrace(point, last);
        this.updateTrace(point);
    }

    padTrace(point, last) {
        if (point[T.time] === last[T.time]) return;

        let l = this.trace.length;
        let timeDiff = point[T.time] - last[T.time];
        let rate = this.minResolution / timeDiff;
        let x = last[T.X];
        let y = last[T.Y];

        while (timeDiff > this.minResolution) {
            this.updateTrace([
                this.trace[l - 1][T.X] + rate * (point[T.X] - x),
                this.trace[l - 1][T.Y] + rate * (point[T.Y] - y),
                this.trace[l - 1][T.time] + this.minResolution,
                "m" // mousemove
            ]);
            timeDiff -= this.minResolution;
            l++;
        }
    }


    updateTrace(point) {
        // console.log(this.trace.length)
        this.trace.push(point);
        if (window.gsyslogger &&
            window.session_config && window.session_config.logTrace)
            window.gsyslogger.addSessionEvent("p", point);
    }

    onmousemove(e) {
        const now = Date.now();
        this.last_point = [e.pageX, e.pageY, now, "m"];
        if (this.trace.length > 0) {
            if (now - _.last(this.trace)[T.time] < this.minResolution)
                return;
        }
        this.pushXYT(e, "m");
    }

    onmousedown(e) {
        this.pushXYT(e, "d");
    }

    onmouseup(e) {
        this.pushXYT(e, "u");
    }

    bind(el) {
        el.addEventListener('mousemove', this.onmousemove.bind(this));
        el.addEventListener('mousedown', this.onmousedown.bind(this));
        el.addEventListener('mouseup', this.onmouseup.bind(this));
    }

}
