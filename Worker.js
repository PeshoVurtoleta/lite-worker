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
// mailbox: a lossless, bounded, single-producer / single-consumer lane over a
// SharedArrayBuffer. Leslie Lamport's SPSC ring (1983): positions only, the
// producer owns TAIL, the consumer owns HEAD, no CAS. Paul Adenot's ringbuf.js
// is the reference SAB SPSC ring for the web. Records are W words of the
// lane's type (Int32 or Float64); push() returns a Smi outcome instead of
// dropping (FULL is a result, never a silent drop or an unbounded buffer).
//
// SELF-CONTAINED, like makeFrameChannel: serialized verbatim into the worker
// runtime via .toString(), so it references only its arguments and globals
// (SharedArrayBuffer, Int32Array, Float64Array, Atomics, Object, Math, Set,
// Error).
//
// LAYOUT (Int32 word index; one lane per SAB in this release; a breaking change
// if it moves -- the torture drives it directly):
//   consumer line   [0] HEAD     next position to read (consumer-only writes)
//                   [1] ARMED    consumer waits on TAIL when nonzero
//   producer line  [32] TAIL     next position to write; the store publishes
//                  [33] FBN      fallback record count before the first SAB
//                                TAIL store (written 0 on the shared path)
//   cold line      [64] GEN      generation, 1 at construction
//                  [65] STATE    0 live, 1 retired
//                  [66] CAP      capacity in records, a power of two
//                  [67] W        words per record
//                  [68] TYPE     0 i32, 1 f64
//                  [69] SLOTS    slab slots (0: no slab)
//                  [70] SLOT_BYTES  bytes per slab slot (0: no slab)
//   payload        byte 384: CAP * W elements of TYPE; record at position p
//                  is elements [(p & (CAP - 1)) * W, + W) of the payload view.
// Positions are Int32 and wrap at 2^31: empty is TAIL === HEAD, full is
// (TAIL - HEAD) | 0 === CAP; every position compare is a difference | 0.
// roundUp128(x) = (x + 127) & ~127. elt = 4 (i32) or 8 (f64).
//   SLOTS = 0:  stride = 384 + roundUp128(CAP * W * elt) + 128 (trailing gap)
//   SLOTS > 0 (fixed here, not built yet): P = 384 + roundUp128(CAP * W * elt);
//     region base B = P + 128; nWords = ceil(SLOTS / 32);
//     hintWords = ceil(nWords / 32); HINT words at B, BITMAP words at
//     B + 4 * hintWords (bit 1 = used, slot = 32 * word + bit; HINT bit j =
//     bitmap word j may be free); slots base S = roundUp128(B + 4 * (hintWords
//     + nWords)) + 128 (SLOT_BYTES % 8 === 0); stride = roundUp128(S + SLOTS *
//     SLOT_BYTES) + 128.
// Hot words sit 128 B apart, so they never share a line or an adjacent-line
// prefetch pair, whatever the SAB base alignment.
//
// Handshake (typed plane, reserved "lw:mb:*"): the producer allocates the SAB,
// writes the cold line and posts lw:mb:sab { id, gen, sab }; it re-posts on
// every lw:mb:hello { id } with its id. The consumer posts lw:mb:hello at
// construction and attaches only to a real SharedArrayBuffer whose byteLength,
// CAP, W, TYPE, SLOTS, SLOT_BYTES match its own layout, whose GEN equals
// msg.gen and whose STATE is 0; anything else is ignored (fail closed).
// ---------------------------------------------------------------------------
function makeMailbox(transport, layout, options) {
  var P = "lite-worker: mailbox";
  function plain(o, what, known) {
    var pr = o !== null && typeof o === "object" ? Object.getPrototypeOf(o) : undefined;
    if ((pr !== Object.prototype && pr !== null) || Object.getOwnPropertySymbols(o).length !== 0) {
      throw new Error(P + " " + what + " must be a plain object");
    }
    var ks = Object.getOwnPropertyNames(o);
    for (var i = 0; i < ks.length; i++) {
      if (known.indexOf(ks[i]) >= 0) continue;
      var best = "", bd = 3;
      for (var j = 0; j < known.length; j++) {
        var a = ks[i].toLowerCase(), b = known[j].toLowerCase(), row = [];
        for (var x = 0; x <= b.length; x++) row.push(x);
        for (var y = 1; y <= a.length; y++) {
          var diag = row[0]; row[0] = y;
          for (var z = 1; z <= b.length; z++) {
            var up = row[z];
            row[z] = Math.min(up + 1, row[z - 1] + 1, diag + (a[y - 1] === b[z - 1] ? 0 : 1));
            diag = up;
          }
        }
        if (row[b.length] < bd) { bd = row[b.length]; best = known[j]; }
      }
      throw new Error(P + " " + what + " has unknown key '" + ks[i] + "'" +
        (best ? " (did you mean '" + best + "'?)" : ""));
    }
    // Only own keys are read: a key inherited from a polluted prototype throws.
    for (var q = 0; q < known.length; q++) {
      if (!Object.prototype.hasOwnProperty.call(o, known[q]) && known[q] in o) {
        throw new Error(P + " " + what + "." + known[q] + " is inherited (only own keys are read)");
      }
    }
  }

  // --- cold validation: every throw fires before the first state write -----
  plain(options, "options", ["role", "mode", "id", "onRecord"]);
  plain(layout, "layout", ["capacity", "words", "type"]);
  var role = options.role;
  if (role !== "producer" && role !== "consumer") {
    throw new Error(P + " needs options.role 'producer' or 'consumer'");
  }
  if (options.mode !== "shared") {
    // "auto" and "transfer" (the credit-window fallback) are not built yet.
    throw new Error(P + " options.mode must be 'shared'");
  }
  var id = options.id === undefined ? 0 : options.id;
  // id <= 2^30 - 1 keeps the id a Smi on every engine (31-bit Smi included).
  if (typeof id !== "number" || (id | 0) !== id || id < 0 || id > 0x3FFFFFFF) {
    throw new Error(P + " options.id must be an integer in [0, 2^30 - 1]");
  }
  var onRecord = options.onRecord;
  if (role === "consumer" && typeof onRecord !== "function") {
    throw new Error(P + " consumer needs options.onRecord(view, offset)");
  }
  if (role === "producer" && onRecord !== undefined) {
    throw new Error(P + " options.onRecord is consumer-only");
  }
  var cap = layout.capacity, w = layout.words;
  if (typeof cap !== "number" || (cap | 0) !== cap || cap < 1 || cap > 0x20000000 || (cap & (cap - 1)) !== 0) {
    throw new Error(P + " layout.capacity must be a power of two in [1, 2^29]");
  }
  if (typeof w !== "number" || (w | 0) !== w || w < 1) {
    throw new Error(P + " layout.words must be an integer >= 1");
  }
  var type = layout.type === undefined ? "i32" : layout.type;
  if (type !== "i32" && type !== "f64") throw new Error(P + " layout.type must be 'i32' or 'f64'");
  var tcode = type === "f64" ? 1 : 0, elt = tcode === 1 ? 8 : 4;
  if (cap * w * elt > 0x40000000) throw new Error(P + " payload CAP * words * " + elt + " exceeds 2^30 bytes");
  var stride = 384 + ((cap * w * elt + 127) & ~127) + 128;
  var coi = (typeof crossOriginIsolated === "boolean") ? crossOriginIsolated : true;
  if (typeof SharedArrayBuffer === "undefined" || typeof Atomics === "undefined" || !coi) {
    throw new Error(
      P + " mode 'shared' needs SharedArrayBuffer" +
      (typeof SharedArrayBuffer === "undefined" ? " (unavailable here)" : " under cross-origin isolation (COOP/COEP)")
    );
  }
  if (!transport || typeof transport.post !== "function" || typeof transport.on !== "function") {
    throw new Error(P + " mode 'shared' needs a transport with post()/on() to hand over the buffer");
  }
  // One live lane per (id, role) per transport: a producer id N and a consumer
  // id N (the two directions) may share a transport. The hidden registry is
  // transport.__lwMb = { producer: Set<id>, consumer: Set<id> }.
  var hold = transport.__lwMb;
  if (hold === undefined && !Object.isExtensible(transport)) {
    throw new Error(P + " needs an extensible transport (it keeps a hidden lane-id registry)");
  }
  if (hold !== undefined && hold[role].has(id)) {
    throw new Error(P + " " + role + " id " + id + " is already live on this transport (dispose() it first)");
  }
  var sabLen = Object.getOwnPropertyDescriptor(SharedArrayBuffer.prototype, "byteLength").get;
  var sab = role === "producer" ? new SharedArrayBuffer(stride) : null;

  // --- state (first write) -------------------------------------------------
  // From here every throw (a transport.on / post that throws) rolls back the
  // registry entry and any listener it installed before rethrowing.
  if (hold === undefined) {
    hold = { producer: new Set(), consumer: new Set() };
    Object.defineProperty(transport, "__lwMb", { value: hold, configurable: true });
  }
  var reg = hold[role];
  reg.add(id);
  var mask = cap - 1, n = cap * w;
  var i32 = null, pv = null;

  if (role === "producer") {
    var gate = 0;                 // 0 live; RETIRED (4) after dispose
    i32 = new Int32Array(sab, 0, 96);
    pv = tcode === 1 ? new Float64Array(sab, 384, n) : new Int32Array(sab, 384, n);
    Atomics.store(i32, 33, 0);
    Atomics.store(i32, 65, 0);
    Atomics.store(i32, 66, cap);
    Atomics.store(i32, 67, w);
    Atomics.store(i32, 68, tcode);
    Atomics.store(i32, 69, 0);
    Atomics.store(i32, 70, 0);
    Atomics.store(i32, 64, 1);
    var meta = { id: id, gen: 1, sab: sab };
    var offHello = null;
    try {
      var offH = transport.on("lw:mb:hello", function (m) {
        if (gate === 0 && m && m.id === id) transport.post("lw:mb:sab", meta);
      });
      if (typeof offH !== "function") throw new Error(P + " transport.on() must return an off() function");
      offHello = offH;
      transport.post("lw:mb:sab", meta);
    } catch (e) {
      gate = 4;
      Atomics.store(i32, 65, 1);    // a consumer that already attached sees STATE retired
      if (offHello) offHello();
      reg.delete(id);
      i32 = null; pv = null; meta.sab = null; sab = null;   // a listener left by a throwing on() must not pin the SAB
      throw e;
    }

    return {
      role: "producer", mode: "shared", id: id,
      capacity: cap, words: w, type: type, byteLength: stride,
      // HOT. Copy W elements src[off .. off + W) into the next slot and publish
      // with one TAIL store. Returns 0 OK, 1 FULL, 4 RETIRED. HEAD and TAIL live
      // in the SAB only (never cached in a JS field); ARMED is re-loaded after
      // the TAIL store, never reused from before it (a lost wake otherwise).
      push: function (src, off) {
        if (gate !== 0) return gate;
        var t = i32[32], h = Atomics.load(i32, 0);
        if (((t - h) | 0) === cap) return 1;
        var b = (t & mask) * w;
        for (var k = 0; k < w; k++) pv[b + k] = src[off + k];
        Atomics.store(i32, 32, (t + 1) | 0);
        if (Atomics.load(i32, 1) !== 0) Atomics.notify(i32, 32, 1);
        return 0;
      },
      dispose: function () {
        if (gate !== 0) return;
        gate = 4;
        Atomics.store(i32, 65, 1);
        offHello();
        reg.delete(id);
        i32 = null; pv = null; meta.sab = null; sab = null;
      }
    };
  }

  var disposed = false, draining = false, offSab = null;
  try {
    var offS = transport.on("lw:mb:sab", function (m) {
      if (disposed || pv !== null || !m || m.id !== id) return;
      var buf = m.sab, len = -1;
      try { len = sabLen.call(buf); } catch (e) { return; }  // not a real SAB
      if (len !== stride) return;
      var hv = new Int32Array(buf, 0, 96);
      if (Atomics.load(hv, 66) !== cap || Atomics.load(hv, 67) !== w || Atomics.load(hv, 68) !== tcode ||
          Atomics.load(hv, 69) !== 0 || Atomics.load(hv, 70) !== 0 ||
          Atomics.load(hv, 64) !== m.gen || Atomics.load(hv, 65) !== 0) return;
      i32 = hv;
      pv = tcode === 1 ? new Float64Array(buf, 384, n) : new Int32Array(buf, 384, n);
      if (offSab !== null) { offSab(); offSab = null; }
    });
    if (typeof offS !== "function") throw new Error(P + " transport.on() must return an off() function");
    offSab = offS;
    if (pv !== null) { offSab(); offSab = null; }   // attached inside on() (a replaying transport)
    transport.post("lw:mb:hello", { id: id });
  } catch (e) {
    // A sync transport may have attached inside post (offSab already null).
    disposed = true;
    if (offSab) { offSab(); offSab = null; }
    reg.delete(id);
    i32 = null; pv = null;
    throw e;
  }

  return {
    role: "consumer", mode: "shared", id: id,
    capacity: cap, words: w, type: type, byteLength: stride,
    get attached() { return pv !== null; },
    // Deliver every record published before this call: onRecord(view, offset)
    // with `view` the lane's payload view (Int32Array or Float64Array over the
    // payload at byte 384) and the record at view[offset .. offset + W). The
    // view is live only inside the callback; HEAD is stored after it returns,
    // so a throw leaves that record to be redelivered by the next drain().
    // Returns the count delivered; 0 when not attached, disposed or nested.
    // A TAIL outside [HEAD, HEAD + CAP] is corrupt: deliver nothing, mark STATE
    // retired, detach (attached -> false) and return 0 (fail closed).
    drain: function () {
      if (pv === null || draining) return 0;
      draining = true;
      var c = 0;
      try {
        var hv = i32, v = pv, h = hv[0], end = Atomics.load(hv, 32), fill = (end - h) | 0;
        if (fill < 0 || fill > cap) {
          Atomics.store(hv, 65, 1);
          i32 = null; pv = null;
          return 0;
        }
        while (h !== end) {
          onRecord(v, (h & mask) * w);
          h = (h + 1) | 0;
          Atomics.store(hv, 0, h);
          c++;
          if (pv === null) break;       // disposed from inside the callback
        }
      } finally {
        draining = false;
      }
      return c;
    },
    dispose: function () {
      if (disposed) return;
      disposed = true;
      if (offSab) { offSab(); offSab = null; }
      reg.delete(id);
      i32 = null; pv = null;
    }
  };
}

// Push outcomes as non-writable statics on a mailbox factory (the export and
// the worker's ctx.mailbox). Self-contained: serialized into the runtime too.
function defineMailboxCodes(fn) {
  var names = ["OK", "FULL", "SLAB_EXHAUSTED", "SLOT_TOO_SMALL", "RETIRED", "REJECTED"];
  for (var i = 0; i < names.length; i++) {
    Object.defineProperty(fn, names[i], { value: i, enumerable: true, writable: false, configurable: false });
  }
  return fn;
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
  // Inject frameChannel, the canvas control and the mailbox into the worker from
  // the single main-side definitions, so there is exactly one implementation of
  // each and no drift between the two sides.
  const frame =
    "\nvar __makeFrameChannel = (" + makeFrameChannel.toString() + ");\n" +
    "__ctx.frameChannel = function (l, o) { return __makeFrameChannel(__ctx, l, o); };\n" +
    "\nvar __makeCanvasControl = (" + makeCanvasControl.toString() + ");\n" +
    "__ctx.onCanvas = function (cb, o) { return __makeCanvasControl(__ctx, cb, o); };\n" +
    "\nvar __makeMailbox = (" + makeMailbox.toString() + ");\n" +
    "__ctx.mailbox = function (l, o) { return __makeMailbox(__ctx, l, o); };\n" +
    "(" + defineMailboxCodes.toString() + ")(__ctx.mailbox);\n";
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

  // --- mailbox -------------------------------------------------------------

  // Build one end of a lossless SPSC mailbox lane (shared mode) over this
  // handle's typed plane. The other end is ctx.mailbox(layout, options) in the
  // worker with the same options.id. Reserved typed messages: "lw:mb:*".
  mailbox(layout, options) {
    return makeMailbox(this, layout, options);
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

/**
 * Build one end of a lossless, bounded, single-producer / single-consumer
 * mailbox lane over a SharedArrayBuffer (Lamport's SPSC ring). `transport` is a
 * WorkerHandle (main thread) or the worker-side `ctx` -- anything exposing
 * `post(type, data)` and `on(type, fn) -> off`; the SAB handshake rides the
 * typed plane ("lw:mb:*" is reserved). Both ends use the same `options.id`
 * (default 0, at most 2^30 - 1); a second live lane with the same id AND role
 * on one transport throws (a producer and a consumer may share an id).
 *
 * `layout` is `{ capacity, words, type }`: capacity a power of two in
 * [1, 2^29], words per record an integer >= 1, type "i32" (default) or "f64".
 * Only `mode: "shared"` exists in this release (it is required).
 *
 * Producer: `push(src, off)` copies `words` elements of `src` from `off` and
 * returns a Smi outcome -- mailbox.OK (0) or mailbox.FULL (1), mailbox.RETIRED
 * (4) after dispose(); never throws on a valid typed array, never drops silently. An i32 lane stores
 * ToInt32 of each value (no validation). Consumer: `drain()` calls
 * `options.onRecord(view, offset)` once per published record, `view` being the
 * lane's payload view (Int32Array / Float64Array) valid only inside the
 * callback, and returns the count delivered.
 *
 * @param {{ post: Function, on: Function }} transport
 * @param {{ capacity: number, words: number, type?: "i32"|"f64" }} layout
 * @param {{ role: "producer"|"consumer", mode: "shared", id?: number, onRecord?: Function }} options
 */
export function mailbox(transport, layout, options) {
  return makeMailbox(transport, layout, options);
}
defineMailboxCodes(mailbox);

export default defineWorker;
