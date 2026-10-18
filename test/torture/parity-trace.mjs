// test/torture/parity-trace.mjs -- the MP tier: behavioural parity against 1.4.0.
//
// trace(mod) drives a lite-worker module through a fixed, deterministic script
// and returns a JSON-safe record of everything an observer can see: the frame
// channel over harness syncPair (transfer + shared), post/call/ctx.frameChannel
// over the loopback Worker mock (every main->worker and worker->main message;
// a main->worker transfer list is honoured, so a transferred buffer detaches),
// post() with a transfer list, terminate() with a call pending, every
// resolution and rejection, the export surface (keys + typeof), the handle
// prototype and the worker ctx keys (+ typeof, from a probe worker kept out of
// the message log). It never records VERSION's value and never records a stack.
//
// mpTier (wired into test/torture.mjs as "MP") gates the LIVE Worker.js against
// a committed frozen copy of 1.4.0 (frozen/Worker-1.4.0.js) and its golden trace
// (golden/parity-1.4.0.json):
//   (a) the frozen copy's sha256 is the pinned one and matches the golden;
//   (b) trace(frozen) deep-equals the golden (the harness itself is stable);
//   (c) trace(live) -- run twice -- deep-equals the golden; a new export key,
//       handle-prototype name or ctx key is tolerated only if it is listed in
//       ALLOWED_NEW (a frozen array, checked at every use);
//   (d) control: the frozen text with its single `dropped++;` statement
//       replaced by `;` (deleting it would make `transport.send(display);`
//       the body of the `if`), imported as a data: URL, MUST diverge from the
//       golden at a `dropped` path; an anchor count other than 1 fails
//       "source drifted". test/parity.test.js runs (a)-(d) plus one spliced
//       control per ported mutant (CONTROLS below) under `npm test`;
//   (e) retention: 64 trace(live) cycles with every handle and channel tracked;
//       tracker.size() returns to 0, every tracked object is collected
//       (WeakRef), and no Worker or Blob URL survives.
// No worker_threads: this is deterministic parity over the in-process mock;
// real-thread SAB behaviour is T3's job. Fails closed without SharedArrayBuffer.
//
// CLI (run once per frozen build, never by the suite):
//   node test/torture/parity-trace.mjs --write [--force]
// imports ONLY the frozen copy, verifies its bytes equal `git show HEAD:Worker.js`,
// refuses to overwrite an existing golden without --force, and writes
// golden/parity-1.4.0.json { schema, frozenSha256, trace }.

import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve as resolvePath } from "node:path";
import { mockRegistry, syncPair, settle, makeTracker, NOOP_CLEANUP } from "./harness.mjs";

// harness.mjs shadows the global URL with a Blob-URL mock, so paths are built
// with node:path and node:url only -- never `new URL(...)`.
const HERE = dirname(fileURLToPath(import.meta.url));
const PKG_ROOT = resolvePath(HERE, "..", "..");
export const FROZEN_PATH = join(HERE, "frozen", "Worker-1.4.0.js");
const GOLDEN_PATH = join(HERE, "golden", "parity-1.4.0.json");
const FROZEN_URL = import.meta.resolve("./frozen/Worker-1.4.0.js");
const LIVE_URL = import.meta.resolve("../../Worker.js");

// sha256 of `git show b595f9e:Worker.js` (the 1.4.0 build). Pinned here as well
// as in the golden, so regenerating the frozen copy AND the golden together
// still fails (a).
export const FROZEN_SHA256 = "b1ddbdf8412da33ed04af6b7ed7970a9a3c2bf127c14e60d0c67cf9e4938f726";
const SCHEMA = 1;
const ANCHOR = "dropped++;";
const RETENTION_CYCLES = 64;
const LATEST_N = 64;

// Export keys, handle-prototype names and worker ctx keys a later rung may ADD
// without breaking parity. Empty at B0; B1 adds "mailbox". Frozen: a test or
// tier cannot widen it at run time.
export const ALLOWED_NEW = Object.freeze([]);

const sha256 = (buf) => createHash("sha256").update(buf).digest("hex");
const sabOk = () => typeof SharedArrayBuffer !== "undefined" && typeof Atomics !== "undefined";

// ---------------------------------------------------------------------------
// canon: any observed value -> a JSON-safe descriptor. Buffers by kind + size,
// views by raw bits (f32 as uint32 words, everything else as bytes), the
// non-JSON numbers spelled out, a `stack` reduced to its typeof.
// ---------------------------------------------------------------------------
function canonView(v) {
  const sab = typeof SharedArrayBuffer !== "undefined" && v.buffer instanceof SharedArrayBuffer;
  if (v instanceof Float32Array) {
    return { $ta: "Float32Array", sab, bits: Array.from(new Uint32Array(v.buffer, v.byteOffset, v.length)) };
  }
  return { $ta: v.constructor.name, sab, bytes: Array.from(new Uint8Array(v.buffer, v.byteOffset, v.byteLength)) };
}

function canon(v, key) {
  if (key === "stack") return { $stack: typeof v };
  if (v === null) return null;
  const t = typeof v;
  if (t === "number") {
    if (v !== v) return { $n: "NaN" };
    if (v === Infinity) return { $n: "Infinity" };
    if (v === -Infinity) return { $n: "-Infinity" };
    if (v === 0 && 1 / v < 0) return { $n: "-0" };
    return v;
  }
  if (t === "string" || t === "boolean") return v;
  if (t === "undefined") return { $u: true };
  if (t === "function") return { $fn: true };
  if (t !== "object") return { $t: t };
  if (typeof SharedArrayBuffer !== "undefined" && v instanceof SharedArrayBuffer) return { $sab: v.byteLength };
  if (v instanceof ArrayBuffer) return { $ab: v.byteLength };
  if (ArrayBuffer.isView(v)) return canonView(v);
  if (Array.isArray(v)) return v.map((x) => canon(x));
  const out = {};
  for (const k of Object.keys(v)) out[k] = canon(v[k], k);
  return out;
}

const errOf = (fn) => {
  try { fn(); return { ok: true }; }
  catch (e) { return { ok: false, name: e && e.name, message: e && e.message }; }
};
const settleOf = (p) => p.then(
  (v) => ({ ok: true, v: canon(v) }),
  (e) => ({ ok: false, name: e && e.name, message: e && e.message })
);

/** First difference between two JSON values, or null. Key ORDER is compared. */
export function firstDiff(a, b, path) {
  if (a === b) return null;
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return { path, a, b };
  if (Array.isArray(a) !== Array.isArray(b)) return { path, a, b };
  if (Array.isArray(a)) {
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i++) {
      const d = firstDiff(a[i], b[i], path + "[" + i + "]");
      if (d) return d;
    }
    return a.length === b.length ? null : { path: path + ".length", a: a.length, b: b.length };
  }
  const ka = Object.keys(a), kb = Object.keys(b);
  const n = Math.min(ka.length, kb.length);
  for (let i = 0; i < n; i++) {
    if (ka[i] !== kb[i]) return { path: path + ".$keys[" + i + "]", a: ka[i], b: kb[i] };
    const d = firstDiff(a[ka[i]], b[kb[i]], path + "." + ka[i]);
    if (d) return d;
  }
  return ka.length === kb.length ? null : { path: path + ".$keys.length", a: ka.length, b: kb.length };
}
const showDiff = (d) => d.path + " (got " + JSON.stringify(d.a) + ", golden " + JSON.stringify(d.b) + ")";

// ---------------------------------------------------------------------------
// Ownership: in the retention phase every handle and channel is tracked at
// creation and untracked at its destroy()/dispose(). A trace that forgets a
// release leaves size() > 0. The tag is a string constant and the cleanup is
// the shared no-op (held-value contract).
// ---------------------------------------------------------------------------
function makeOwner(tracker, stats) {
  let open = 0;
  return {
    hold(obj, tag) {
      if (!tracker) return null;
      open++;
      if (stats) { stats.holds++; if (stats.refs) stats.refs.push(new WeakRef(obj)); }
      return tracker.track(obj, NOOP_CLEANUP, tag);
    },
    drop(rec) {
      if (!tracker) return;
      open--;
      tracker.untrack(rec);
    },
    get open() { return open; },
  };
}

// ---------------------------------------------------------------------------
// Section 1: frameChannel over harness syncPair.
// ---------------------------------------------------------------------------
const fillState = { v: 0 };
function fill(f) { for (let k = 0; k < f.length; k++) f[k] = fillState.v * 4 + k + 0.5; }

// Field order follows the plan: read values, dropped, free, byteLength, mode,
// shared, hasNew, received, lastFrame, torn, published.
function snap(prod, cons) {
  const hasNewBefore = cons.hasNew;
  const r = cons.read();
  return {
    read: canon(r),
    dropped: canon(cons.dropped),
    free: canon(prod ? prod.free : undefined),
    byteLength: canon(cons.byteLength),
    mode: canon(cons.mode),
    shared: canon(cons.shared),
    hasNew: canon(cons.hasNew),
    received: canon(cons.received),
    lastFrame: canon(cons.lastFrame),
    torn: canon(cons.torn),
    published: canon(prod ? prod.published : undefined),
    hasNewBefore: canon(hasNewBefore),
    prodMode: canon(prod ? prod.mode : undefined),
  };
}
function peek(prod, cons) {
  return { free: canon(prod.free), hasNew: canon(cons.hasNew), dropped: canon(cons.dropped), published: canon(prod.published) };
}

function fcSection(frameChannel, typed, layout, own) {
  const out = {};
  const [pt, ct] = syncPair(typed);
  const prod = frameChannel(pt, layout, { role: "producer", capacity: 2, count: 2 });
  const rp = own.hold(prod, "mp-channel");
  const cons = frameChannel(ct, layout, { role: "consumer", capacity: 2, count: 2 });
  const rc = own.hold(cons, "mp-channel");
  out.prodKeys = Object.keys(prod);
  out.consKeys = Object.keys(cons);
  out.prod = { kind: prod.kind, stride: prod.stride, capacity: prod.capacity, byteLength: prod.byteLength, count: prod.count };
  out.init = snap(prod, cons);

  // Latest-wins: one seed frame + one seed read (shared mode counts drops as the
  // FRAMES delta between reads, so a read must precede the burst), then N
  // produces and ONE read: the consumer holds the newest, dropped == N - 1.
  fillState.v = 1;
  const seed = prod.produce(fill);
  const seedSnap = snap(prod, cons);
  let produced = 0;
  for (let i = 0; i < LATEST_N; i++) { fillState.v = 2 + i; if (prod.produce(fill) === true) produced++; }
  out.latest = { seed, seedSnap, produced, after: snap(prod, cons) };

  // Recycle loop: a read every third frame.
  const steps = [];
  for (let i = 0; i < 24; i++) {
    fillState.v = 100 + i;
    const ok = prod.produce(fill);
    steps.push({ ok, s: (i % 3 === 2) ? snap(prod, cons) : peek(prod, cons) });
  }
  out.recycle = steps;

  // readInto: the tear-free copy path, a fresh frame per call except one.
  const probe = cons.read();
  const dst = new probe.constructor(probe.length);
  const into = [];
  for (let i = 0; i < 4; i++) {
    fillState.v = 200 + i;
    const ok = prod.produce(fill);
    if (i !== 1) into.push({ ok, into: cons.readInto(dst), dst: canon(dst), s: peek(prod, cons), lastFrame: cons.lastFrame });
  }
  out.readInto = into;

  prod.dispose(); own.drop(rp);
  cons.dispose(); own.drop(rc);
  fillState.v = 999;
  out.disposed = {
    free: prod.free,
    produce: prod.produce(fill),
    read: canon(cons.read()),
    dropped: cons.dropped,
    again: [errOf(() => prod.dispose()), errOf(() => cons.dispose())],
  };
  return out;
}

function fcErrors(frameChannel) {
  const [pt] = syncPair(false);
  return [
    errOf(() => frameChannel(pt, 4, { capacity: 2 })),
    errOf(() => frameChannel(pt, 0, { role: "producer", capacity: 2 })),
    errOf(() => frameChannel(pt, { bytes: 0 }, { role: "producer" })),
    errOf(() => frameChannel(pt, "x", { role: "producer" })),
  ];
}

// ---------------------------------------------------------------------------
// Section 2: post / call / ctx.frameChannel over the loopback Worker mock.
// The two bodies are serialized via toString(): self-contained, no module scope.
// ---------------------------------------------------------------------------
function rpcBody(ctx) {
  ctx.on("echo", function (d) { return d; });
  ctx.on("sink", function (d) { ctx.post("sunk", d instanceof ArrayBuffer ? d.byteLength : -1); });
  ctx.on("async", function (d) { return Promise.resolve(d + 1); });
  ctx.on("throws", function () { throw new TypeError("mp boom"); });
  ctx.on("hang", function () { return undefined; });
  ctx.on("ping", function (d) { ctx.post("pong", { n: d.n + 1 }); });
  ctx.onRaw(function (b) { ctx.send(b); });
}

function fcBody(ctx) {
  var prod = null, v = 0;
  function fillW(f) { for (var k = 0; k < f.length; k++) f[k] = v * 4 + k + 0.25; }
  function run(n) {
    var r = [];
    for (var i = 0; i < n; i++) { v++; r.push(prod.produce(fillW)); }
    return { r: r, free: prod.free, published: prod.published, mode: prod.mode, keys: Object.keys(prod) };
  }
  ctx.on("fc", function (d) {
    prod = ctx.frameChannel(4, { role: "producer", capacity: 2, count: 2, mode: d.mode });
    return run(d.n);
  });
  ctx.on("more", function (d) { return run(d.n); });
}

// Worker ctx keys + typeof, from a probe worker spawned on the plain mock (so the
// probe never enters the message log), sorted by key.
function ctxProbeBody(ctx) {
  ctx.post("keys", Object.keys(ctx).map(function (k) { return [k, typeof ctx[k]]; }));
}
async function ctxKeysOf(defineWorker, own) {
  let keys = null;
  const hp = defineWorker(ctxProbeBody);
  const rp = own.hold(hp, "mp-handle");
  hp.on("keys", (d) => { keys = d; });
  hp.spawn();
  await new Promise((r) => setImmediate(r));
  hp.destroy(); own.drop(rp);
  if (!Array.isArray(keys)) throw new Error("MP: the ctx-keys probe never reported");
  return keys.slice().sort((x, y) => (x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0));
}

async function handleSection(mod, own) {
  const { defineWorker } = mod;
  const out = {};
  const log = [];
  const Base = globalThis.Worker;
  out.ctxKeys = await ctxKeysOf(defineWorker, own);
  // Installed only for the duration of this section, restored in finally.
  class RecWorker extends Base {
    constructor(url, opts) {
      super(url, opts);
      let h = null;
      Object.defineProperty(this, "onmessage", {
        configurable: true,
        enumerable: true,
        get() { return h; },
        set(fn) {
          h = typeof fn === "function"
            ? (ev) => { log.push({ dir: "w>m", data: canon(ev.data) }); fn(ev); }
            : fn;
        },
      });
    }
    postMessage(data, transfer) {
      log.push({ dir: "m>w", data: canon(data), transfer: transfer ? transfer.length : -1 });
      // Honour the transfer list as postMessage does: the sender's buffers detach.
      const sent = transfer && transfer.length ? structuredClone(data, { transfer }) : data;
      super.postMessage(sent, transfer);
    }
  }
  globalThis.Worker = RecWorker;
  try {
    out.defineWorkerBad = errOf(() => defineWorker(1));

    // before spawn
    const h0 = defineWorker(rpcBody);
    const r0 = own.hold(h0, "mp-handle");
    out.fresh = { spawned: h0.spawned, destroyed: h0.destroyed };
    out.postBeforeSpawn = errOf(() => h0.post("echo", 1));
    out.sendBeforeSpawn = errOf(() => h0.send(new Uint8Array([1])));
    out.beforeSpawn = await settleOf(h0.call("echo", 1));
    h0.destroy(); own.drop(r0);

    // a live rpc handle
    const h = defineWorker(rpcBody, { name: "mp" });
    const r1 = own.hold(h, "mp-handle");
    out.spawnReturnsHandle = h.spawn() === h;
    out.live = { spawned: h.spawned, destroyed: h.destroyed };
    const pongs = [];
    const raws = [];
    const sunk = [];
    h.on("pong", (d) => { pongs.push(canon(d)); });
    h.on("sunk", (d) => { sunk.push(canon(d)); });
    h.onRaw((b) => { raws.push(canon(b)); });
    h.post("ping", { n: 7 });
    // post() with a transfer list: the sender's buffer detaches (byteLength 0).
    const ab3 = new ArrayBuffer(4);
    h.post("sink", ab3, [ab3]);
    out.postTransfer = { byteLength: ab3.byteLength };
    out.echo = await settleOf(h.call("echo", { a: 1, b: [2, 3], s: "x", z: -0, u: undefined, n: null }));
    out.async = await settleOf(h.call("async", 41));
    out.throws = await settleOf(h.call("throws"));
    out.noHandler = await settleOf(h.call("nope", 1));
    out.timeout = await settleOf(h.call("hang", 1, { timeout: 5 }));
    h.send(new Float32Array([1.5, -2]));
    h.send(new ArrayBuffer(4));
    h.send(new Uint8Array([1, 2, 3]), []);
    out.flush = await settleOf(h.call("echo", 0));
    out.pongs = pongs;
    out.sunk = sunk;
    out.raws = raws;
    h.destroy(); own.drop(r1);
    out.dead = { spawned: h.spawned, destroyed: h.destroyed };
    out.afterDestroy = await settleOf(h.call("echo", 1));
    out.postAfterDestroy = errOf(() => h.post("echo", 1));
    out.destroyAgain = errOf(() => h.destroy());
    out.spawnAfterDestroy = errOf(() => h.spawn());

    // terminate() with a call pending, the dead window, respawn, destroy with a
    // call pending.
    const ht = defineWorker(rpcBody).spawn();
    const rt = own.hold(ht, "mp-handle");
    const hang = ht.call("hang", 1);
    const lc = {};
    lc.terminateReturnsHandle = ht.terminate() === ht;
    lc.terminated = { spawned: ht.spawned, destroyed: ht.destroyed };
    lc.pendingAtTerminate = await settleOf(hang);
    lc.terminateAgain = ht.terminate() === ht;
    lc.callAfterTerminate = await settleOf(ht.call("echo", 2));
    lc.respawnReturnsHandle = ht.spawn() === ht;
    lc.callAfterRespawn = await settleOf(ht.call("echo", 3));
    const hang2 = ht.call("hang", 2);
    lc.destroyReturns = canon(ht.destroy());
    lc.pendingAtDestroy = await settleOf(hang2);
    own.drop(rt);
    out.lifecycle = lc;

    // ctx.frameChannel (worker producer) -> handle.frameChannel (main consumer)
    out.fc = {};
    for (const mode of ["transfer", "auto"]) {
      const hf = defineWorker(fcBody).spawn();
      const rf = own.hold(hf, "mp-handle");
      const cons = hf.frameChannel(4, { role: "consumer", capacity: 2, count: 2, mode });
      const rc = own.hold(cons, "mp-channel");
      const rec = { keys: Object.keys(cons) };
      rec.first = await settleOf(hf.call("fc", { mode, n: 5 }));
      rec.snap1 = snap(null, cons);
      rec.more = await settleOf(hf.call("more", { n: 3 }));
      rec.snap2 = snap(null, cons);
      cons.dispose(); own.drop(rc);
      hf.destroy(); own.drop(rf);
      out.fc[mode] = rec;
    }

    // surface: export keys + typeof (never VERSION's value), handle prototype
    const hp = defineWorker(rpcBody);
    out.proto = Object.getOwnPropertyNames(Object.getPrototypeOf(hp)).sort();
    hp.destroy();
    out.messages = log;
  } finally {
    globalThis.Worker = Base;
  }
  return out;
}

/**
 * Run the parity script against a lite-worker module namespace. With a tracker,
 * every handle/channel is tracked and released (stats.holds counts them).
 */
export async function trace(mod, tracker, stats) {
  if (!sabOk()) throw new Error("MP: no SharedArrayBuffer/Atomics -- the shared-mode trace cannot run (fail closed)");
  const own = makeOwner(tracker || null, stats || null);
  const t = {};
  t.exports = Object.keys(mod).sort().map((k) => [k, typeof mod[k]]);
  t.defaultIsDefineWorker = mod.default === mod.defineWorker;
  const fc = mod.frameChannel;
  t.fc = {
    transfer: {
      f32: fcSection(fc, false, 4, own),
      obj: fcSection(fc, false, { stride: 3, capacity: 2 }, own),
      bytes: fcSection(fc, false, { bytes: 8 }, own),
    },
    shared: {
      f32: fcSection(fc, true, 4, own),
      obj: fcSection(fc, true, { stride: 3, capacity: 2 }, own),
      bytes: fcSection(fc, true, { bytes: 8 }, own),
    },
    errors: fcErrors(fc),
  };
  t.handle = await handleSection(mod, own);
  if (own.open !== 0) throw new Error("MP: trace left " + own.open + " handles/channels un-released");
  return JSON.parse(JSON.stringify(t));
}

/**
 * Drop the allowed additions (default ALLOWED_NEW) from a live trace before
 * comparing it. The list must be a frozen array of strings none of which the
 * golden already holds (fail closed).
 */
export function allowNew(t, golden, allowed = ALLOWED_NEW) {
  if (!Array.isArray(allowed) || !Object.isFrozen(allowed)) {
    throw new Error("MP: ALLOWED_NEW is not a frozen array");
  }
  for (const k of allowed) {
    if (typeof k !== "string") {
      throw new Error("MP: ALLOWED_NEW entry " + String(k) + " is not a string");
    }
    if (golden.exports.some((e) => e[0] === k) || golden.handle.proto.includes(k) ||
        golden.handle.ctxKeys.some((e) => e[0] === k)) {
      throw new Error("MP: ALLOWED_NEW entry '" + k + "' is already in the golden");
    }
  }
  t.exports = t.exports.filter((e) => !allowed.includes(e[0]));
  t.handle.proto = t.handle.proto.filter((n) => !allowed.includes(n));
  t.handle.ctxKeys = t.handle.ctxKeys.filter((e) => !allowed.includes(e[0]));
  return t;
}

/**
 * Splice one anchor in a source text; any count other than `count` (default 1)
 * fails "source drifted". Every occurrence is replaced.
 */
export function splice(text, anchor, replacement, count = 1) {
  const n = text.split(anchor).length - 1;
  if (n !== count) {
    throw new Error("MP: source drifted -- '" + anchor + "' occurs " + n + " times, expected " + count);
  }
  return text.split(anchor).join(replacement);
}

/** Import a source text as a module (data: URL; Worker.js has no imports). */
export const importText = (text) =>
  import("data:text/javascript;base64," + Buffer.from(text, "utf8").toString("base64"));

/**
 * Ported mutant controls (B0x), spliced into the FROZEN text: each MUST diverge
 * from the golden at exactly `path` (its first diff). (d)'s dropped++ control
 * stays in mutantSource().
 */
export const CONTROLS = Object.freeze([
  Object.freeze({ id: "post-transfer", anchor: "w.postMessage(tx, transfer || NO_TRANSFER);",
    to: "w.postMessage(tx, NO_TRANSFER);", path: "trace.handle.postTransfer.byteLength" }),
  Object.freeze({ id: "terminate-message", anchor: "\"lite-worker: worker terminated\"",
    to: "\"lite-worker: worker stopped\"",
    path: "trace.handle.lifecycle.pendingAtTerminate.message" }),
  Object.freeze({ id: "ctx-extra-key", anchor: "  close: function () { self.close(); }\n};",
    to: "  close: function () { self.close(); },\n  mpExtraKey: function () {}\n};",
    path: "trace.handle.ctxKeys[2][0]" }),
  Object.freeze({ id: "ctx-typeof", anchor: "offRaw: __offRaw", to: "offRaw: null",
    path: "trace.handle.ctxKeys[3][1]" }),
]);

/** The control's mutant source: the single `dropped++;` statement replaced by `;`. */
export function mutantSource(text) {
  const n = text.split(ANCHOR).length - 1;
  if (n !== 1) throw new Error("MP: source drifted -- '" + ANCHOR + "' occurs " + n + " times, expected 1");
  // Leave an empty statement so `if (!readSinceSwap)` keeps its own body.
  return text.replace(ANCHOR, ";");
}

export function loadGolden(frozenSha, frozenVersion) {
  if (!existsSync(GOLDEN_PATH)) throw new Error("MP (a): golden missing at test/torture/golden/parity-1.4.0.json (fail closed)");
  const text = readFileSync(GOLDEN_PATH, "utf8");
  const g = JSON.parse(text);
  if (g.schema !== SCHEMA) throw new Error("MP (a): golden schema " + g.schema + " != " + SCHEMA);
  if (g.frozenSha256 !== frozenSha) throw new Error("MP (a): golden frozenSha256 " + g.frozenSha256 + " != frozen copy " + frozenSha);
  if (typeof frozenVersion !== "string" || frozenVersion.length === 0 || text.includes(frozenVersion)) {
    throw new Error("MP (a): the golden records VERSION's value (or VERSION is not a string)");
  }
  const keys = g.trace.exports.map((e) => e[0]).join(",");
  if (keys !== "VERSION,default,defineWorker,frameChannel") throw new Error("MP (a): golden export keys " + keys);
  for (const mode of ["transfer", "shared"]) {
    const d = g.trace.fc[mode].f32.latest.after.dropped;
    if (d !== LATEST_N - 1) throw new Error("MP (a): golden " + mode + " latest-wins dropped " + d + " != " + (LATEST_N - 1));
  }
  return g;
}

/** The MP torture tier. */
export async function mpTier() {
  if (!sabOk()) throw new Error("MP: no SharedArrayBuffer/Atomics (fail closed)");
  const mockWorker = globalThis.Worker;

  // (a) frozen copy == pinned sha == golden.frozenSha256
  const frozenBytes = readFileSync(FROZEN_PATH);
  const sha = sha256(frozenBytes);
  if (sha !== FROZEN_SHA256) throw new Error("MP (a): frozen copy sha256 " + sha + " != pinned " + FROZEN_SHA256);
  const frozen = await import(FROZEN_URL);
  const golden = loadGolden(sha, frozen.VERSION);

  // (b) the script is stable against the build it was recorded from
  let d = firstDiff(await trace(frozen), golden.trace, "trace");
  if (d) throw new Error("MP (b): trace(frozen) != golden at " + showDiff(d));

  // (c) the live build behaves like 1.4.0, twice
  const live = await import(LIVE_URL);
  for (let run = 1; run <= 2; run++) {
    d = firstDiff(allowNew(await trace(live), golden.trace), golden.trace, "trace");
    if (d) throw new Error("MP (c): trace(live) run " + run + " != golden at " + showDiff(d));
  }
  if (globalThis.Worker !== mockWorker) throw new Error("MP: the recording Worker was not uninstalled");

  // (d) control: the anchor guard fails closed, and the mutant MUST diverge
  const text = frozenBytes.toString("utf8");
  for (const bad of [text.split(ANCHOR).join(""), text + "\n// " + ANCHOR + "\n"]) {
    let msg = "";
    try { mutantSource(bad); } catch (e) { msg = String(e && e.message); }
    if (!msg.includes("source drifted")) throw new Error("MP (d): an anchor count != 1 did not fail 'source drifted'");
  }
  const mutText = mutantSource(text);
  if (mutText === text) throw new Error("MP (d): mutant equals the frozen source");
  const mut = await import("data:text/javascript;base64," + Buffer.from(mutText, "utf8").toString("base64"));
  const md = firstDiff(await trace(mut), golden.trace, "trace");
  if (md === null) throw new Error("MP (d): control decorative -- the dropped++ mutant traced equal to the golden");
  if (!md.path.includes("dropped")) throw new Error("MP (d): control diverged at " + showDiff(md) + ", not at a dropped counter");

  // (e) retention: 64 cycles of trace(live), every handle and channel tracked
  const { tracker, leaks, warns } = makeTracker();
  const w0 = mockRegistry.liveWorkers(), u0 = mockRegistry.liveUrls();
  const stats = { holds: 0, refs: [] };
  if (typeof globalThis.gc !== "function") throw new Error("MP (e): needs --expose-gc (fail closed)");
  for (let i = 0; i < RETENTION_CYCLES; i++) {
    d = firstDiff(allowNew(await trace(live, tracker, stats), golden.trace), golden.trace, "trace");
    if (d) throw new Error("MP (e): retention cycle " + i + " != golden at " + showDiff(d));
  }
  await settle();
  await settle();
  const alive = stats.refs.filter((r) => r.deref() !== undefined).length;
  const size = tracker.size();
  const findings = tracker.audit();
  const dw = mockRegistry.liveWorkers() - w0, du = mockRegistry.liveUrls() - u0;
  process.stderr.write("    MP retention: cycles=" + RETENTION_CYCLES +  " holds=" + stats.holds + " alive=" + alive + " size=" + size +
    " findings=" + findings.length + " leaks=" + leaks.length + " warnings=" + warns.length +
    " workers+" + dw + " urls+" + du + " | control diverged at " + md.path + "\n");
  if (stats.holds === 0 || stats.holds % RETENTION_CYCLES !== 0) throw new Error("MP (e): tracked " + stats.holds + " objects -- the retention phase tracked nothing per cycle");
  if (alive !== 0) throw new Error("MP (e): " + alive + " of " + stats.refs.length + " tracked objects survived gc (retained)");
  if (size !== 0) throw new Error("MP (e): tracker.size() " + size + " != 0 after " + RETENTION_CYCLES + " cycles");
  if (findings.length !== 0) throw new Error("MP (e): " + findings.length + " leak findings");
  if (leaks.length !== 0) throw new Error("MP (e): leaks: " + leaks.join(", "));
  if (warns.length !== 0) throw new Error("MP (e): tracker warnings: " + warns.join(", "));
  if (dw !== 0 || du !== 0) throw new Error("MP (e): mock registry delta workers=" + dw + " urls=" + du);
}

// ---------------------------------------------------------------------------
// CLI: --write [--force]. Cold code; never run by the suite.
// ---------------------------------------------------------------------------
function pretty(v, ind) {
  const flat = JSON.stringify(v);
  if (v === null || typeof v !== "object" || flat.length + ind.length <= 100) return flat;
  const next = ind + "  ";
  if (Array.isArray(v)) return "[\n" + v.map((x) => next + pretty(x, next)).join(",\n") + "\n" + ind + "]";
  return "{\n" + Object.keys(v).map((k) => next + JSON.stringify(k) + ": " + pretty(v[k], next)).join(",\n") + "\n" + ind + "}";
}

async function cli(args) {
  for (const a of args) {
    if (a !== "--write" && a !== "--force") {
      process.stderr.write("parity-trace: unknown argument '" + a + "' (known: --write, --force)\n");
      return 2;
    }
  }
  if (!args.includes("--write")) {
    process.stderr.write("usage: node test/torture/parity-trace.mjs --write [--force]\n");
    return 2;
  }
  const force = args.includes("--force");
  if (!sabOk()) { process.stderr.write("parity-trace: no SharedArrayBuffer/Atomics (fail closed)\n"); return 1; }
  const frozenBytes = readFileSync(FROZEN_PATH);
  let head;
  try {
    head = execFileSync("git", ["show", "HEAD:./Worker.js"], { cwd: PKG_ROOT, maxBuffer: 1 << 24, stdio: ["ignore", "pipe", "pipe"] });
  } catch (e) {
    process.stderr.write("parity-trace: cannot read HEAD:Worker.js via git (" + String(e && e.message).split("\n")[0] + ")\n");
    return 1;
  }
  if (!frozenBytes.equals(head)) { process.stderr.write("parity-trace: frozen copy != git show HEAD:Worker.js -- refusing\n"); return 1; }
  const sha = sha256(frozenBytes);
  if (sha !== FROZEN_SHA256) { process.stderr.write("parity-trace: frozen sha256 " + sha + " != pinned " + FROZEN_SHA256 + "\n"); return 1; }
  if (existsSync(GOLDEN_PATH) && !force) {
    process.stderr.write("parity-trace: refusing to overwrite " + GOLDEN_PATH + " (pass --force)\n");
    return 1;
  }
  const frozen = await import(FROZEN_URL);
  const t1 = await trace(frozen);
  const t2 = await trace(frozen);
  const dd = firstDiff(t2, t1, "trace");
  if (dd) { process.stderr.write("parity-trace: trace(frozen) is not deterministic at " + showDiff(dd) + "\n"); return 1; }
  const text = pretty({ schema: SCHEMA, frozenSha256: sha, trace: t1 }, "") + "\n";
  const back = JSON.parse(text);
  if (firstDiff(back.trace, t1, "trace") !== null) { process.stderr.write("parity-trace: golden does not round-trip\n"); return 1; }
  if (text.includes(frozen.VERSION)) { process.stderr.write("parity-trace: the trace recorded VERSION's value -- refusing\n"); return 1; }
  mkdirSync(dirname(GOLDEN_PATH), { recursive: true });
  writeFileSync(GOLDEN_PATH, text);
  process.stderr.write("parity-trace: wrote " + GOLDEN_PATH + " (" + text.length + " bytes, frozen " + sha + ")\n");
  return 0;
}

if (process.argv[1] !== undefined && resolvePath(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await cli(process.argv.slice(2));
}
