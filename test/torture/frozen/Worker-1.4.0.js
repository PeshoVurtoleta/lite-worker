/**
 * @zakkster/lite-worker
 * Zero-GC per-frame worker channel. Inline worker core (v1.0.0).
 *
 * Two transports on one Worker:
 *   - raw:  send()/onRaw()  -> transferable ArrayBuffers, no envelope, zero
 *           allocation in steady state. This is the 60fps hot path.
 *   - typed: post()/call()/on() -> {t,i,d} envelope for control-plane messages
 *           (init/config/request-response). Allocates only for call() bookkeeping.
 *
 * @license MIT
 * @copyright Zahary Shinikchiev
 */

/** Package version. Kept in three-place sync with package.json and CHANGELOG.md. */
export const VERSION = "1.4.0";

// Reused across every handle: an empty transfer list. postMessage reads it
// synchronously, so a shared frozen instance is safe and allocates nothing.
const NO_TRANSFER = Object.freeze([]);

// ---------------------------------------------------------------------------
// Worker-side runtime. Serialized verbatim into the Blob. Runs inside the
// worker global scope. Defines `__ctx` and hands it to the user module fn.
// Mirrors the main-side transport: same raw/envelope discrimination, same
// scratch-object reuse so the sim thread also stays allocation-free per tick.
// ---------------------------------------------------------------------------
const WORKER_RUNTIME = `"use strict";
var __h = new Map();          // type -> Set(handler)
var __raw = new Set();        // raw buffer handlers
var __tx = { t: "", i: 0, d: null };   // scratch push/reply-less envelope
var __rp = { i: 0, d: null };          // scratch reply envelope
var __ep = { i: 0, e: null };          // scratch error-reply envelope
var __t1 = [ null ];                    // reused single-slot transfer list

function __xfer(buf, transfer) {
  if (transfer !== undefined) return transfer;
  var ab = buf instanceof ArrayBuffer ? buf
         : (ArrayBuffer.isView(buf) ? buf.buffer : null);
  if (ab) { __t1[0] = ab; return __t1; }
  return [];
}

function __post(type, data, transfer) {
  __tx.t = type; __tx.i = 0; __tx.d = data;
  self.postMessage(__tx, transfer || []);
  __tx.d = null;
}
function __send(buf, transfer) {
  var t = __xfer(buf, transfer);
  self.postMessage(buf, t);
  __t1[0] = null;
}
function __reply(id, data, transfer) {
  __rp.i = id; __rp.d = data;
  var t = transfer || (function () {
    var ab = data instanceof ArrayBuffer ? data
           : (ArrayBuffer.isView(data) ? data.buffer : null);
    if (ab) { __t1[0] = ab; return __t1; }
    return [];
  })();
  self.postMessage(__rp, t);
  __rp.d = null; __t1[0] = null;
}
function __replyErr(id, err) {
  __ep.i = id;
  __ep.e = {
    name: (err && err.name) || "Error",
    message: (err && err.message) || String(err),
    stack: (err && err.stack) || ""
  };
  self.postMessage(__ep);
  __ep.e = null;
}

function __on(type, fn) {
  var s = __h.get(type); if (!s) { s = new Set(); __h.set(type, s); } s.add(fn);
  return function () { s.delete(fn); };
}
function __off(type, fn) { var s = __h.get(type); if (s) s.delete(fn); }
function __onRaw(fn) { __raw.add(fn); return function () { __raw.delete(fn); }; }
function __offRaw(fn) { __raw.delete(fn); }

var __ctx = {
  on: __on, off: __off,
  onRaw: __onRaw, offRaw: __offRaw,
  post: __post, send: __send,
  close: function () { self.close(); }
};

self.onmessage = function (ev) {
  var data = ev.data;
  if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
    __raw.forEach(function (fn) { fn(data); });
    return;
  }
  var type = data.t;
  var id = data.i | 0;
  var s = __h.get(type);
  if (!s || s.size === 0) {
    if (id > 0) __replyErr(id, new Error("lite-worker: no handler for '" + type + "'"));
    return;
  }
  if (id > 0) {
    var replied = false;
    var reply = function (rd, tr) { if (replied) return; replied = true; __reply(id, rd, tr); };
    s.forEach(function (fn) {
      try {
        var r = fn(data.d, reply);
        if (r && typeof r.then === "function") {
          r.then(
            function (v) { if (!replied && v !== undefined) reply(v); },
            function (e) { if (!replied) { replied = true; __replyErr(id, e); } }
          );
        } else if (!replied && r !== undefined) {
          reply(r);
        }
      } catch (e) {
        if (!replied) { replied = true; __replyErr(id, e); }
      }
    });
  } else {
    s.forEach(function (fn) {
      try { fn(data.d, undefined); }
      catch (e) { __post("error", { message: String((e && e.message) || e) }); }
    });
  }
};
`;

// ---------------------------------------------------------------------------
// frameChannel: a bounded, latest-wins, ping-pong frame channel over the raw
// transport. One pool of pre-allocated ArrayBuffers cycles producer->consumer
// and back via ownership transfer -- no per-frame data allocation, no structured
// clone of objects, and memory that cannot grow (the pool is fixed).
//
// Direction carries meaning, so no envelope/tag is needed: a raw buffer arriving
// at the consumer is a fresh frame; a raw buffer arriving at the producer is a
// recycled buffer returning to the pool.
//
// SELF-CONTAINED. This function is serialized verbatim into the worker runtime
// via .toString(), so it MUST NOT reference anything from module scope -- only
// its arguments and globals (ArrayBuffer, Float32Array, Uint8Array).
//
// `transport` is anything with { send(buffer, transfer?), onRaw(fn) -> off } --
// both the main-side WorkerHandle and the worker-side ctx satisfy that shape.
// ---------------------------------------------------------------------------
function makeFrameChannel(transport, layout, options) {
  var opts = options || {};
  var role = opts.role;
  if (role !== "producer" && role !== "consumer") {
    throw new Error("lite-worker: frameChannel needs options.role 'producer' or 'consumer'");
  }
  var count = opts.count | 0;
  if (count < 2) count = 2;

  // Normalize layout -> { kind, stride, capacity, byteLength }.
  var kind = "f32", stride = 0, capacity = 0, byteLength = 0;
  if (typeof layout === "number") {
    stride = layout | 0;
    capacity = opts.capacity | 0;
    if (stride <= 0 || capacity <= 0) {
      throw new Error("lite-worker: frameChannel(stride) needs a positive stride and options.capacity");
    }
    byteLength = capacity * stride * 4;
  } else if (layout && typeof layout === "object" && typeof layout.bytes === "number") {
    kind = "bytes";
    byteLength = layout.bytes | 0;
    if (byteLength <= 0) throw new Error("lite-worker: frameChannel({bytes}) needs a positive byte length");
  } else if (layout && typeof layout === "object") {
    stride = layout.stride | 0;
    capacity = (layout.capacity | 0) || (opts.capacity | 0);
    if (stride <= 0 || capacity <= 0) {
      throw new Error("lite-worker: frameChannel({stride,capacity}) needs positive stride and capacity");
    }
    byteLength = capacity * stride * 4;
  } else {
    throw new Error("lite-worker: frameChannel(layout) must be a stride number, {stride,capacity}, or {bytes}");
  }

  function makeView(buf, byteOffset) {
    return kind === "bytes"
      ? new Uint8Array(buf, byteOffset || 0, byteLength)
      : new Float32Array(buf, byteOffset || 0, byteLength >> 2);
  }

  // --- mode: "auto" (default) | "shared" (SAB; alias "sab") | "transfer" ----
  // The shared path needs SharedArrayBuffer, which browsers gate behind
  // cross-origin isolation (COOP/COEP). `crossOriginIsolated` is a boolean in
  // browsers and undefined in Node, so a non-boolean means "no COI concept
  // here, allow it"; an explicit false means the page didn't opt in.
  var wanted = opts.mode || "auto";
  if (wanted === "sab") wanted = "shared";
  if (wanted !== "auto" && wanted !== "shared" && wanted !== "transfer") {
    throw new Error("lite-worker: frameChannel options.mode must be 'auto', 'shared', or 'transfer'");
  }
  var coi = (typeof crossOriginIsolated === "boolean") ? crossOriginIsolated : true;
  var canShare = typeof SharedArrayBuffer !== "undefined" && typeof Atomics !== "undefined" && coi;
  // The handshake that hands the SAB across rides the typed plane, so a bare
  // { send, onRaw } transport can't negotiate one.
  var canTalk = typeof transport.post === "function" && typeof transport.on === "function";
  if (wanted === "shared" && !canShare) {
    throw new Error(
      "lite-worker: frameChannel mode 'shared' needs SharedArrayBuffer" +
      (typeof SharedArrayBuffer === "undefined" ? " (unavailable here)" : " under cross-origin isolation (COOP/COEP)")
    );
  }
  if (wanted === "shared" && !canTalk) {
    throw new Error("lite-worker: frameChannel mode 'shared' needs a transport with post()/on() to hand over the buffer");
  }
  var useShared = wanted !== "transfer" && canShare && canTalk;

  // Shared-memory layout: a 4-slot Int32 header followed by `count` frame slots.
  //   [0] SEQ    seqlock -- even = stable, odd = a publish is in progress
  //   [1] PUB    index of the slot holding the newest published frame
  //   [2] FRAMES total frames published (lets a consumer spot missed frames)
  //   [3] reserved
  var HDR_I32 = 4, HDR_BYTES = 16, SEQ = 0, PUB = 1, FRAMES = 2;
  var hdr = null, slots = null, sab = null;

  function attachShared(buffer) {
    sab = buffer;
    hdr = new Int32Array(buffer, 0, HDR_I32);
    slots = [];
    for (var s = 0; s < count; s++) slots.push(makeView(buffer, HDR_BYTES + s * byteLength));
  }

  var disposed = false;
  var off = null;

  if (role === "producer") {
    if (useShared) {
      // Allocate the shared region and hand it across once. After this there is
      // ZERO postMessage traffic on the data path: publishing is three atomics.
      attachShared(new SharedArrayBuffer(HDR_BYTES + count * byteLength));
      var meta = { sab: sab, stride: stride, capacity: capacity, kind: kind, count: count, byteLength: byteLength };
      transport.post("lw:fc:sab", meta);
      // A consumer built after this point missed the handshake; it announces
      // itself and we re-send, so construction order doesn't matter.
      var offHello = transport.on("lw:fc:hello", function () {
        if (!disposed) transport.post("lw:fc:sab", meta);
      });

      return {
        role: "producer", mode: "shared", shared: true,
        kind: kind, stride: stride, capacity: capacity, byteLength: byteLength, count: count,
        // Write the next slot and publish it with a seqlock: bump SEQ odd,
        // swing PUB, bump SEQ even. A reader that observes an odd SEQ or a
        // changed SEQ knows it raced and retries.
        produce: function (fill) {
          if (disposed) return false;
          var w = (Atomics.load(hdr, PUB) + 1) % count;
          fill(slots[w], sab);
          Atomics.add(hdr, SEQ, 1);      // -> odd: publish in progress
          Atomics.store(hdr, PUB, w);
          Atomics.add(hdr, SEQ, 1);      // -> even: stable
          Atomics.add(hdr, FRAMES, 1);
          return true;                    // shared mode never starves
        },
        get free() { return count; },
        get published() { return hdr ? Atomics.load(hdr, FRAMES) : 0; },
        dispose: function () {
          if (disposed) return;
          disposed = true;
          offHello();
          hdr = null; slots = null; sab = null;
        }
      };
    }

    // The producer owns every buffer to begin with; they flow to the consumer
    // and cycle back. `free` = buffers currently owned and writable.
    var free = [];
    for (var i = 0; i < count; i++) free.push(new ArrayBuffer(byteLength));

    off = transport.onRaw(function (buf) {
      if (disposed) return;
      var ab = buf instanceof ArrayBuffer ? buf : (buf && buf.buffer) || null;
      if (ab && ab.byteLength === byteLength) free.push(ab); // returned to pool
    });

    return {
      role: "producer", mode: "transfer", shared: false,
      kind: kind, stride: stride, capacity: capacity, byteLength: byteLength, count: count,
      // Fill the next free buffer via fill(view, buffer) and transfer it to the
      // consumer. Returns false when no buffer is free -- the frame is dropped and
      // the caller's simulation simply advances to the next tick. Latest-wins.
      produce: function (fill) {
        if (disposed || free.length === 0) return false;
        var buf = free.pop();
        try { fill(makeView(buf), buf); }
        catch (e) { free.push(buf); throw e; } // keep the pool intact on user error
        transport.send(buf); // auto-transfers the ArrayBuffer
        return true;
      },
      get free() { return free.length; },
      dispose: function () {
        if (disposed) return;
        disposed = true;
        if (off) off();
        free.length = 0;
      }
    };
  }

  // consumer: hold the freshest frame as `display`; supersede + recycle older
  // frames the instant a newer one arrives, so nothing queues.
  var display = null;
  var view = null;           // cached view over `display`, rebuilt only on swap
  var readSinceSwap = true;
  var dropped = 0;
  var received = 0;          // transfer mode: raw frames arrived (onRaw swaps)
  var onFrame = typeof opts.onFrame === "function" ? opts.onFrame : null;
  var lastFrame = -1;        // shared mode: last FRAMES counter observed
  var torn = 0;              // shared mode: reads that lost the seqlock race

  // A consumer always starts on the transferable path and upgrades in place if
  // the producer hands over a SharedArrayBuffer, so neither side has to agree
  // up front and construction order doesn't matter.
  var offSab = null;
  if (wanted !== "transfer" && canTalk) {
    offSab = transport.on("lw:fc:sab", function (m) {
      // Fail closed: `lw:fc:*` is a reserved namespace, but the typed plane is
      // shared, so a spoofed message could carry a plain ArrayBuffer as `sab`.
      // Only a real SharedArrayBuffer earns the upgrade; anything else stays on
      // the transferable ring.
      if (disposed || hdr || !m || !m.sab || !(m.sab instanceof SharedArrayBuffer)) return;
      if (m.byteLength !== byteLength || (m.count | 0) !== count) return; // layout mismatch: stay on transfer
      attachShared(m.sab);
      if (off) { off(); off = null; }   // the raw path is no longer used
      display = null; view = null;
    });
    transport.post("lw:fc:hello", null); // in case the producer already sent it
  }

  off = transport.onRaw(function (buf) {
    if (disposed || hdr) return;
    var ab = buf instanceof ArrayBuffer ? buf : (buf && buf.buffer) || null;
    if (!ab || ab.byteLength !== byteLength) return;
    received++;                        // one raw frame arrived (post-guard, pre-swap)
    if (display !== null) {
      if (!readSinceSwap) dropped++;   // previous frame never read before supersede
      transport.send(display);         // recycle the old display back to producer
    }
    display = ab;
    view = makeView(ab);
    readSinceSwap = false;
    if (onFrame) { readSinceSwap = true; onFrame(view, ab); }
  });

  return {
    role: "consumer",
    get mode() { return hdr ? "shared" : "transfer"; },
    get shared() { return !!hdr; },
    kind: kind, stride: stride, capacity: capacity, byteLength: byteLength, count: count,
    // Non-destructive read of the freshest frame, or null before the first one.
    // Safe to call every rAF and allocates nothing (views are cached: one per
    // slot in shared mode, one per buffer in transfer mode).
    read: function () {
      if (hdr) {
        // Seqlock acquire: an odd SEQ means a publish is mid-flight, so take the
        // previous stable slot rather than a half-swung pointer.
        var s0 = Atomics.load(hdr, SEQ);
        if (s0 & 1) { torn++; s0 = Atomics.load(hdr, SEQ); }
        var f = Atomics.load(hdr, FRAMES);
        if (f === 0) return null;
        if (lastFrame >= 0 && f > lastFrame + 1) dropped += f - lastFrame - 1;
        lastFrame = f;
        return slots[Atomics.load(hdr, PUB)];
      }
      if (display === null) return null;
      readSinceSwap = true;
      return view;
    },
    // Tear-free snapshot: copies the newest frame into `dst` and re-checks the
    // seqlock, retrying if the producer published mid-copy. Use this when the
    // consumer holds the data across a yield; read() is the fast path for a
    // renderer that consumes it immediately. Returns false if nothing is
    // published yet or the retries were exhausted.
    readInto: function (dst) {
      if (!hdr) {
        if (display === null) return false;
        dst.set(view); readSinceSwap = true; return true;
      }
      for (var attempt = 0; attempt < 4; attempt++) {
        var s0 = Atomics.load(hdr, SEQ);
        if (s0 & 1) continue;                       // publish in progress
        var f = Atomics.load(hdr, FRAMES);
        if (f === 0) return false;
        dst.set(slots[Atomics.load(hdr, PUB)]);
        if (Atomics.load(hdr, SEQ) === s0) {        // nothing published mid-copy
          if (lastFrame >= 0 && f > lastFrame + 1) dropped += f - lastFrame - 1;
          lastFrame = f;
          return true;
        }
        torn++;
      }
      return false;
    },
    get hasNew() {
      if (hdr) return Atomics.load(hdr, FRAMES) > (lastFrame < 0 ? 0 : lastFrame);
      return display !== null && !readSinceSwap;
    },
    get dropped() { return dropped; },
    /** Shared mode only: reads that raced an in-flight publish and retried. */
    get torn() { return torn; },
    // Consumer telemetry. transfer: frames arrived on the ring (arrival-driven).
    // shared: frames observed as of the last read (read-driven, NOT publish-driven --
    // it only advances when you read, so it is not a delivery/drift counter). 0
    // before any activity, never null.
    get received() { return hdr ? (lastFrame < 0 ? 0 : lastFrame) : received; },
    // shared: FRAMES seq value at the last read (-1 before the first read).
    // transfer: mirrors `received` (-1 before the first arrival).
    get lastFrame() { return hdr ? lastFrame : (received === 0 ? -1 : received); },
    dispose: function () {
      if (disposed) return;
      disposed = true;
      if (off) off();
      if (offSab) offSab();
      display = null;
      view = null;
      hdr = null; slots = null; sab = null;
    }
  };
}

// ---------------------------------------------------------------------------
// Worker-side canvas control. Serialized into the runtime via .toString(), so it
// MUST be self-contained: only its arguments and worker globals (setTimeout,
// clearTimeout, performance). Paired with the main-side WorkerHandle.adoptCanvas,
// which owns transferControlToOffscreen + resize/visibility forwarding.
//
// Protocol (reserved "lw:canvas*" typed messages, main -> worker):
//   lw:canvas       { canvas, w, h, dpr }  first adoption (canvas is transferred)
//   lw:canvas:size  { w, h, dpr }          a resize (canvas.width/height applied)
//   lw:canvas:vis   { visible }            tab visibility (pauses/resumes frame())
// ---------------------------------------------------------------------------
function makeCanvasControl(ctx, cb, options) {
  var opt = options || {};
  var canvas = null, dpr = 1, visible = true, adopted = false;
  var resizeCbs = [], visCbs = [];
  var loopFn = null;
  var loopInterval = opt.fps ? (1000 / opt.fps) : (opt.interval || (1000 / 60));
  var loopUseRaf = false, loopHandle = 0, loopIsRaf = false, loopLast = 0, loopStopped = true;
  var now = function () { return typeof performance !== "undefined" ? performance.now() : Date.now(); };
  // Modern browsers expose requestAnimationFrame on the worker global alongside
  // OffscreenCanvas, which is vsync-paced and self-throttles when hidden. Prefer
  // it; fall back to setTimeout where it's absent or when an explicit rate is set.
  var raf = (typeof requestAnimationFrame === "function") ? requestAnimationFrame : null;
  var caf = (typeof cancelAnimationFrame === "function") ? cancelAnimationFrame : null;

  function step(ts) {
    loopHandle = 0;
    if (loopStopped || !visible || !loopFn) return;
    var t = (loopIsRaf && typeof ts === "number") ? ts : now();
    var dt = t - loopLast; loopLast = t;
    loopFn(dt);
    schedule();
  }
  function schedule() {
    if (loopStopped || !visible || !loopFn || loopHandle) return;
    if (loopUseRaf) { loopIsRaf = true; loopHandle = raf(step); }
    else { loopIsRaf = false; loopHandle = setTimeout(step, loopInterval); }
  }
  function resumeLoop() {
    if (loopStopped || !visible || !loopFn || loopHandle) return;
    loopLast = now();
    schedule();
  }
  function pauseLoop() {
    if (!loopHandle) return;
    if (loopIsRaf) { if (caf) caf(loopHandle); } else { clearTimeout(loopHandle); }
    loopHandle = 0;
  }

  var ctl = {
    get canvas() { return canvas; },
    get width() { return canvas ? canvas.width : 0; },
    get height() { return canvas ? canvas.height : 0; },
    get dpr() { return dpr; },
    get visible() { return visible; },
    // Register a resize listener. Fired after canvas.width/height is applied.
    onResize: function (fn) { resizeCbs.push(fn); return function () { var i = resizeCbs.indexOf(fn); if (i >= 0) resizeCbs.splice(i, 1); }; },
    // Register a visibility listener (true when the owning tab is visible).
    onVisibility: function (fn) { visCbs.push(fn); return function () { var i = visCbs.indexOf(fn); if (i >= 0) visCbs.splice(i, 1); }; },
    // Start a render loop that auto-pauses when the tab is hidden (workers have
    // no requestAnimationFrame, so this is timer-driven). Returns a stop function.
    frame: function (fn, o) {
      loopFn = fn;
      var wantRate = !!(o && (o.fps || o.interval));
      if (o && o.fps) loopInterval = 1000 / o.fps; else if (o && o.interval) loopInterval = o.interval;
      // rAF for smooth vsync pacing, unless the caller asked for a specific rate.
      loopUseRaf = !wantRate && !!raf;
      loopStopped = false;
      resumeLoop();
      return function () { loopStopped = true; pauseLoop(); loopFn = null; };
    },
    pause: function () { visible = false; pauseLoop(); },
    resume: function () { visible = true; resumeLoop(); }
  };

  var offCanvas = ctx.on("lw:canvas", function (d) {
    canvas = d.canvas; dpr = d.dpr || 1;
    canvas.width = d.w; canvas.height = d.h;
    if (!adopted) { adopted = true; cb(canvas, ctl); }
  });
  var offSize = ctx.on("lw:canvas:size", function (d) {
    if (!canvas) return;
    dpr = d.dpr || dpr;
    canvas.width = d.w; canvas.height = d.h;
    for (var i = 0; i < resizeCbs.length; i++) resizeCbs[i](canvas.width, canvas.height, dpr);
  });
  var offVis = ctx.on("lw:canvas:vis", function (d) {
    visible = !!d.visible;
    if (visible) resumeLoop(); else pauseLoop();
    for (var i = 0; i < visCbs.length; i++) visCbs[i](visible);
  });

  ctl.dispose = function () {
    loopStopped = true; pauseLoop(); loopFn = null;
    offCanvas(); offSize(); offVis();
    resizeCbs.length = 0; visCbs.length = 0;
  };
  return ctl;
}

function buildSource(moduleFn) {
  const body = moduleFn.toString();
  // Inject frameChannel and the canvas control into the worker from the single
  // main-side definitions, so there is exactly one implementation of each and no
  // drift between the two sides.
  const frame =
    "\nvar __makeFrameChannel = (" + makeFrameChannel.toString() + ");\n" +
    "__ctx.frameChannel = function (l, o) { return __makeFrameChannel(__ctx, l, o); };\n" +
    "\nvar __makeCanvasControl = (" + makeCanvasControl.toString() + ");\n" +
    "__ctx.onCanvas = function (cb, o) { return __makeCanvasControl(__ctx, cb, o); };\n";
  // ;( ... )(__ctx) invokes the user module fn with the worker channel.
  return WORKER_RUNTIME + frame + "\n;(" + body + ")(__ctx);\n";
}

// ---------------------------------------------------------------------------
// Main-side handle.
// ---------------------------------------------------------------------------
class WorkerHandle {
  constructor(source, options) {
    this._source = source;
    this._type = options.type === "module" ? "module" : "classic";
    this._name = options.name || "";
    this._onError = typeof options.onError === "function" ? options.onError : null;

    this._worker = null;
    this._dead = false;

    this._h = new Map();        // type -> Set(handler) for worker->main pushes
    this._raw = new Set();      // raw buffer handlers
    this._pending = new Map();  // id -> { resolve, reject, timer }
    this._nextId = 1;

    // Reused scratch objects: keep the main thread allocation-free on post()
    // and on the raw send() path.
    this._tx = { t: "", i: 0, d: null };
    this._t1 = [null];

    this._onMessage = this._onMessage.bind(this);
    this._onErr = this._onErr.bind(this);
  }

  get spawned() { return this._worker !== null; }
  get destroyed() { return this._dead; }

  spawn() {
    if (this._dead) {
      throw new Error("lite-worker: handle destroyed; call defineWorker() again");
    }
    if (this._worker) return this;
    const blob = new Blob([this._source], { type: "text/javascript" });
    const url = URL.createObjectURL(blob);
    const opts = {};
    if (this._type === "module") opts.type = "module";
    if (this._name) opts.name = this._name;
    const w = new Worker(url, opts);
    // The worker script is fetched synchronously during construction, so the
    // object URL can be revoked right away. A short-lived worker that is never
    // terminated/destroyed therefore leaks nothing: there is no retained URL to
    // pile up across the document's lifetime. Re-spawn mints a fresh URL.
    URL.revokeObjectURL(url);
    w.onmessage = this._onMessage;
    w.onerror = this._onErr;
    w.onmessageerror = this._onErr;
    this._worker = w;
    return this;
  }

  // --- raw transport (zero-GC hot path) -----------------------------------

  send(buffer, transfer) {
    const w = this._worker;
    if (!w) return;
    if (transfer === undefined) {
      const ab = buffer instanceof ArrayBuffer ? buffer
        : (ArrayBuffer.isView(buffer) ? buffer.buffer : null);
      if (ab) {
        this._t1[0] = ab;
        w.postMessage(buffer, this._t1);
        this._t1[0] = null;
        return;
      }
      w.postMessage(buffer, NO_TRANSFER);
      return;
    }
    w.postMessage(buffer, transfer);
  }

  onRaw(fn) { this._raw.add(fn); return () => this._raw.delete(fn); }
  offRaw(fn) { this._raw.delete(fn); }

  // --- typed transport (control plane) ------------------------------------

  post(type, data, transfer) {
    const w = this._worker;
    if (!w) return;
    const tx = this._tx;
    tx.t = type; tx.i = 0; tx.d = data === undefined ? null : data;
    w.postMessage(tx, transfer || NO_TRANSFER);
    tx.d = null;
  }

  call(type, data, opts) {
    if (this._dead) {
      return Promise.reject(new Error("lite-worker: handle destroyed"));
    }
    const w = this._worker;
    if (!w) {
      return Promise.reject(new Error("lite-worker: call('" + type + "') before spawn()"));
    }
    const id = this._nextId++;
    const timeout = opts && opts.timeout;
    const transfer = (opts && opts.transfer) || NO_TRANSFER;
    return new Promise((resolve, reject) => {
      const entry = { resolve, reject, timer: 0 };
      if (timeout > 0) {
        entry.timer = setTimeout(() => {
          this._pending.delete(id);
          reject(new Error("lite-worker: call('" + type + "') timed out after " + timeout + "ms"));
        }, timeout);
      }
      this._pending.set(id, entry);
      const tx = this._tx;
      tx.t = type; tx.i = id; tx.d = data === undefined ? null : data;
      w.postMessage(tx, transfer);
      tx.d = null;
    });
  }

  on(type, fn) {
    let s = this._h.get(type);
    if (!s) { s = new Set(); this._h.set(type, s); }
    s.add(fn);
    return () => { s.delete(fn); };
  }
  off(type, fn) { const s = this._h.get(type); if (s) s.delete(fn); }

  // --- frame channel -------------------------------------------------------

  // Build a bounded, latest-wins frame channel over this handle's raw transport.
  // One frame channel owns the raw stream -- don't also use send()/onRaw()
  // directly while it is live. Typed post()/call()/on() remain free for control.
  // For a producer channel on the main thread, spawn() first (sends are no-ops
  // before spawn). The worker side uses ctx.frameChannel(layout, options).
  frameChannel(layout, options) {
    return makeFrameChannel(this, layout, options);
  }

  // Hand a canvas to the worker via transferControlToOffscreen and keep it in
  // sync: a ResizeObserver forwards size changes (in device pixels, with dpr),
  // and visibilitychange forwards tab visibility so the worker's render loop can
  // auto-pause when hidden. Pair with ctx.onCanvas(cb) in the worker. Returns a
  // controller with { resize(), pause(), resume(), dispose() }.
  adoptCanvas(canvasEl, options) {
    if (!canvasEl || typeof canvasEl.transferControlToOffscreen !== "function") {
      throw new Error("lite-worker: adoptCanvas needs a canvas supporting transferControlToOffscreen()");
    }
    const opt = options || {};
    const maxDpr = opt.maxDpr || 2;
    const hasWin = typeof window !== "undefined";
    const hasDoc = typeof document !== "undefined";
    const dims = () => {
      const dpr = Math.min((hasWin && window.devicePixelRatio) || 1, maxDpr);
      const r = canvasEl.getBoundingClientRect();
      return { w: Math.max(1, (r.width * dpr) | 0), h: Math.max(1, (r.height * dpr) | 0), dpr };
    };

    const off = canvasEl.transferControlToOffscreen();
    const d0 = dims();
    off.width = d0.w; off.height = d0.h;
    this.post("lw:canvas", { canvas: off, w: d0.w, h: d0.h, dpr: d0.dpr }, [off]);

    const pushSize = () => { const d = dims(); this.post("lw:canvas:size", d); };
    const pushVis = () => this.post("lw:canvas:vis", { visible: !(hasDoc && document.hidden) });

    let ro = null;
    if (typeof ResizeObserver !== "undefined") { ro = new ResizeObserver(pushSize); ro.observe(canvasEl); }
    if (hasWin) window.addEventListener("resize", pushSize);
    if (hasDoc) document.addEventListener("visibilitychange", pushVis);
    pushVis(); // initial visibility

    return {
      resize: pushSize,
      pause: () => this.post("lw:canvas:vis", { visible: false }),
      resume: () => this.post("lw:canvas:vis", { visible: true }),
      dispose: () => {
        if (ro) ro.disconnect();
        if (hasWin) window.removeEventListener("resize", pushSize);
        if (hasDoc) document.removeEventListener("visibilitychange", pushVis);
      }
    };
  }

  // --- lifecycle -----------------------------------------------------------

  terminate() {
    const w = this._worker;
    if (w) {
      w.onmessage = null;
      w.onerror = null;
      w.onmessageerror = null;
      w.terminate();
      this._worker = null;
    }
    if (this._pending.size) {
      const err = new Error("lite-worker: worker terminated");
      this._pending.forEach((p) => { if (p.timer) clearTimeout(p.timer); p.reject(err); });
      this._pending.clear();
    }
    return this;
  }

  destroy() {
    if (this._dead) return;
    this.terminate();
    this._dead = true;
    this._h.clear();
    this._raw.clear();
  }

  // --- internal ------------------------------------------------------------

  _onMessage(ev) {
    const data = ev.data;
    if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
      this._raw.forEach((fn) => fn(data));
      return;
    }
    const type = data.t;
    if (type !== undefined) {
      const s = this._h.get(type);
      if (s) s.forEach((fn) => fn(data.d));
      return;
    }
    // reply envelope: { i, d } or { i, e }
    const id = data.i | 0;
    const p = this._pending.get(id);
    if (!p) return;
    this._pending.delete(id);
    if (p.timer) clearTimeout(p.timer);
    if (data.e) {
      const e = data.e;
      const err = new Error(e.message);
      if (e.name) err.name = e.name;
      if (e.stack) err.stack = e.stack;
      p.reject(err);
    } else {
      p.resolve(data.d);
    }
  }

  _onErr(ev) {
    const err = (ev && ev.error) instanceof Error
      ? ev.error
      : new Error((ev && ev.message) || "lite-worker: worker error");
    if (this._onError) this._onError(err);
    const s = this._h.get("error");
    if (s) s.forEach((fn) => fn(err));
    // A worker error (a load/parse failure, or an uncaught error) will never
    // reply to an in-flight call -- reject them now rather than let them hang to
    // their timeout. Handler-level throws during a call are replied to
    // individually and don't reach here, so this only catches genuine failures.
    if (this._pending.size) {
      this._pending.forEach((p) => { if (p.timer) clearTimeout(p.timer); p.reject(err); });
      this._pending.clear();
    }
  }
}

/**
 * Create a worker from a self-contained module function. The function is
 * serialized (via .toString()) into a Blob URL, so there is no separate file
 * and no bundler configuration. It cannot close over anything from the calling
 * module scope; it receives its channel as the single `ctx` argument.
 *
 * The worker is not created until spawn() is called.
 *
 * @param {(ctx: any) => void} moduleFn self-contained worker body
 * @param {{ type?: "classic"|"module", name?: string, onError?: (e: Error) => void }} [options]
 * @returns {WorkerHandle}
 */
export function defineWorker(moduleFn, options = {}) {
  if (typeof moduleFn !== "function") {
    throw new TypeError("lite-worker: defineWorker(moduleFn) requires a function");
  }
  return new WorkerHandle(buildSource(moduleFn), options);
}

/**
 * Build a bounded, latest-wins frame channel over a raw transport. `transport`
 * is a WorkerHandle (main thread) or the worker-side `ctx` -- anything exposing
 * `send(buffer, transfer?)` and `onRaw(fn) -> off`.
 *
 * A fixed pool of `count` ArrayBuffers (default 2) ping-pongs between producer
 * and consumer via ownership transfer: no per-frame data allocation, no object
 * clone, and memory that cannot grow. If the consumer falls behind, intermediate
 * frames are dropped (latest-wins) and nothing queues unboundedly.
 *
 * `layout` is a Float32 stride (floats per instance, e.g. lite-gl LAYOUT.POINT),
 * `{ stride, capacity }`, or `{ bytes }` for a generic byte buffer.
 *
 * @param {{ send: Function, onRaw: Function }} transport
 * @param {number | { stride: number, capacity: number } | { bytes: number }} layout
 * @param {{ role: "producer"|"consumer", count?: number, capacity?: number, onFrame?: Function }} options
 */
export function frameChannel(transport, layout, options) {
  return makeFrameChannel(transport, layout, options);
}

export default defineWorker;
