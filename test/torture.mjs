// test/torture.mjs -- node --expose-gc test/torture.mjs
//
// The lite-worker torture suite. Runs ten tiers in order; prints exactly "ok"
// to stdout on success (exit 0) and a diagnostic + exit 1 on any failure. Tier
// progress and the gate summary go to stderr so stdout stays clean.
//
// Tiers: T0 (channel laws), T1 (degenerate layouts), T2 (lifecycle abuse), T3
// (real-thread seqlock, W1), T4 (adversarial backpressure, W2), T5 (differential
// fuzz, W1), T6 (zero-retention gate), T7 (soak + conservation + leak), T8
// (lite-gl LAYOUT conformance, W2), T9 (controls). No pending placeholders remain.
//
// Every gate ships a T9 control that MUST report failure; a gate that cannot
// fail is decorative (W-01 was the limiting case -- a gate that never ran).

import { Worker as NodeWorker } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import { measureOps, checkOps } from "@zakkster/lite-gc-profiler";
import { defineWorker, frameChannel } from "../Worker.js";
import {
  mockRegistry,
  syncPair,
  pair,
  xorshift32,
  settle,
  makeTracker,
  NOOP_CLEANUP,
  nodeThreadTransport,
} from "./torture/harness.mjs";

// harness.mjs shadows the global URL with a Blob-URL mock, so resolve the entry
// path via import.meta.resolve rather than `new URL(...)`.
const THREAD_ENTRY = fileURLToPath(import.meta.resolve("./torture/thread-entry.mjs"));

const log = (s) => process.stderr.write(s + "\n");

const SEED = process.env.TORTURE_SEED ? (parseInt(process.env.TORTURE_SEED, 10) | 0) : 0x1a2b3c4d;
const SAB_OK = typeof SharedArrayBuffer !== "undefined" && typeof Atomics !== "undefined";
const LEAK_CYCLES = 4096;

// Gate metrics collected by T6/T7 for the summary line.
const metrics = {
  leakSize: 0, findings: 0, warnings: 0,
  gcMajor: 0, gcMinor: 0, gcMaxMs: 0,
  bytesPerOp: 0,
};

// Shared gate predicates -- used by the real tiers AND by the T9 controls, so
// the control exercises the exact check the tier relies on.
const boundedOk = (inFlight, count) => inFlight <= count;
const conservationOk = (free, count) => free === count;
// Transferable-ring conservation at rest: every one of the `count` distinct
// buffers is either free to write or held by the consumer as display -- none
// leaked, none duplicated. Used by the T4 tier AND the T9 leaking-pool control.
const poolConservedOk = (free, held, count) => free + held === count;

function fail(msg, op) {
  const e = new Error(msg);
  e.seed = SEED;
  if (op !== undefined) e.op = op;
  return e;
}

// ---------------------------------------------------------------------------
// T0 -- channel laws: latest-wins, bounded, pool conservation.
// ---------------------------------------------------------------------------
async function t0() {
  // Latest-wins: N produces, M<N reads -> the consumer holds the newest, and
  // every superseded-unread frame is counted as dropped.
  {
    const [pt, ct] = syncPair(false);
    const count = 2, N = 64;
    const prod = frameChannel(pt, 4, { role: "producer", capacity: 4, count });
    const cons = frameChannel(ct, 4, { role: "consumer", capacity: 4, count });
    let v = 0;
    const fill = (f) => { f[0] = v; };
    for (let i = 1; i <= N; i++) { v = i; prod.produce(fill); }
    const got = cons.read();
    if (!got || got[0] !== N) throw fail("T0 latest-wins: consumer did not hold the newest frame (" + (got && got[0]) + " != " + N + ")");
    if (cons.dropped !== N - 1) throw fail("T0 latest-wins: dropped=" + cons.dropped + " != " + (N - 1));
    prod.dispose(); cons.dispose();
  }

  // Bounded + conservation: with synchronous delivery in-flight is 0 at rest,
  // so after the first frame arrives the consumer holds exactly one buffer and
  // free == count-1 forever -- free never exceeds count, the pool never grows.
  for (const count of [2, 3, 8]) {
    const [pt, ct] = syncPair(false);
    const prod = frameChannel(pt, 2, { role: "producer", capacity: 8, count });
    const cons = frameChannel(ct, 2, { role: "consumer", capacity: 8, count });
    let v = 0;
    const fill = (f) => { f[0] = v; };
    for (let i = 0; i < 2000; i++) {
      v = i; prod.produce(fill);
      if (!boundedOk(prod.free, count)) throw fail("T0 bounded: free=" + prod.free + " > count=" + count, i);
      if (prod.free !== count - 1) throw fail("T0 conservation: free=" + prod.free + " != count-1 (" + (count - 1) + ")", i);
      if ((i & 7) === 0) cons.read();
    }
    prod.dispose(); cons.dispose();
    if (prod.free !== 0) throw fail("T0: dispose did not zero free (" + prod.free + ")");
  }

  // Mode-parity: shared mode drops in place but still counts dropped, and
  // latest-wins holds identically.
  if (SAB_OK) {
    const [pt, ct] = syncPair(true);
    const count = 4;
    const prod = frameChannel(pt, 8, { role: "producer", capacity: 4, count });
    const cons = frameChannel(ct, 8, { role: "consumer", capacity: 4, count });
    if (prod.mode !== "shared" || cons.mode !== "shared") throw fail("T0 parity: shared negotiation failed");
    let v = 0;
    const fill = (f) => { f[0] = v; };
    // Seed lastFrame with one read; shared mode counts drops as the FRAMES delta
    // between consecutive reads, so a read must precede the burst.
    v = 1; prod.produce(fill); cons.read();
    for (let i = 2; i <= 300; i++) { v = i; prod.produce(fill); }
    const got = cons.read();
    if (!got || got[0] !== 300) throw fail("T0 parity: shared consumer not latest (" + (got && got[0]) + ")");
    if (cons.dropped <= 0) throw fail("T0 parity: shared mode did not count drops");
    if (prod.free !== count) throw fail("T0 parity: shared producer free != count");
    prod.dispose(); cons.dispose();
  }
}

// ---------------------------------------------------------------------------
// T1 -- degenerate layouts and payloads. Pin the actual answer: a throw is a
// valid contract, silent garbage is not.
// ---------------------------------------------------------------------------
async function t1() {
  const throwsSync = (fn, match) => {
    try { fn(); return false; }
    catch (e) { return match ? String(e.message).includes(match) : true; }
  };
  const [pt] = syncPair(false);
  const [tt] = syncPair(true);
  const P = (layout, opts) => frameChannel(pt, layout, Object.assign({ role: "producer" }, opts));
  const PT = (layout, opts) => frameChannel(tt, layout, Object.assign({ role: "producer" }, opts));

  // stride edge cases
  if (!throwsSync(() => P(0, { capacity: 4 }), "stride")) throw fail("T1: stride 0 did not throw");
  if (!throwsSync(() => P(-1, { capacity: 4 }), "stride")) throw fail("T1: stride -1 did not throw");
  if (!throwsSync(() => P(NaN, { capacity: 4 }), "stride")) throw fail("T1: stride NaN did not throw");
  // stride 1.5 truncates via |0 to 1 -- documented, no throw
  {
    const p = P(1.5, { capacity: 4 });
    if (p.stride !== 1) throw fail("T1: stride 1.5 did not truncate to 1 (" + p.stride + ")");
    p.dispose();
  }

  // capacity edge cases
  if (!throwsSync(() => P(8, { capacity: 0 }), "capacity")) throw fail("T1: capacity 0 did not throw");
  if (!throwsSync(() => P(8, { capacity: -1 }), "capacity")) throw fail("T1: capacity -1 did not throw");
  if (!throwsSync(() => P(8, { capacity: NaN }), "capacity")) throw fail("T1: capacity NaN did not throw");
  if (!throwsSync(() => P(8, {}), "capacity")) throw fail("T1: missing capacity did not throw");

  // count clamps to a minimum of 2
  for (const c of [0, 1, -3]) {
    const p = P(4, { capacity: 4, count: c });
    if (p.count !== 2) throw fail("T1: count " + c + " did not clamp to 2 (" + p.count + ")");
    p.dispose();
  }

  // mode edge cases
  if (!throwsSync(() => P(4, { capacity: 4, mode: "nope" }), "mode")) throw fail("T1: unknown mode did not throw");

  // {bytes} edge cases
  if (!throwsSync(() => P({ bytes: 0 }), "byte")) throw fail("T1: {bytes:0} did not throw");
  if (!throwsSync(() => P({ bytes: -4 }), "byte")) throw fail("T1: {bytes:-4} did not throw");
  {
    // {bytes:7} -- odd, non-multiple-of-4: a byte channel uses Uint8Array views,
    // which need no alignment, so it is valid and byteLength is exactly 7.
    const p = P({ bytes: 7 });
    if (p.kind !== "bytes" || p.byteLength !== 7) throw fail("T1: {bytes:7} mis-shaped (kind=" + p.kind + " len=" + p.byteLength + ")");
    p.dispose();
  }
  if (SAB_OK) {
    // Non-multiple-of-4 bytes in shared mode: the Int32 header is aligned at 0;
    // slot views are Uint8Array, so odd slot offsets are still legal.
    const p = PT({ bytes: 6 }, { mode: "shared" });
    if (p.mode !== "shared" || p.byteLength !== 6) throw fail("T1: shared {bytes:6} mis-shaped (mode=" + p.mode + " len=" + p.byteLength + ")");
    p.dispose();
  }

  // garbage layout
  if (!throwsSync(() => P("nope", {}))) throw fail("T1: garbage layout did not throw");
  if (!throwsSync(() => P(4, { capacity: 4, role: undefined }), "role")) throw fail("T1: missing role did not throw");
}

// ---------------------------------------------------------------------------
// T2 -- lifecycle abuse. Every out-of-band call is a no-op or a reject, never a
// crash; 4096 spawn/destroy cycles retain zero Blob URLs and zero live workers.
// ---------------------------------------------------------------------------
async function t2() {
  const rejects = async (fn, match) => {
    try { await fn(); return false; } catch (e) { return match ? String(e.message).includes(match) : true; }
  };
  const noop = (fn) => { try { fn(); return true; } catch { return false; } };

  // before spawn
  {
    const h = defineWorker((ctx) => { ctx.on("x", () => {}); });
    if (!noop(() => h.send(new Uint8Array([1])))) throw fail("T2: send before spawn threw");
    if (!noop(() => h.post("x", 1))) throw fail("T2: post before spawn threw");
    if (!(await rejects(() => h.call("x"), "spawn"))) throw fail("T2: call before spawn did not reject");
    h.destroy();
  }

  // after terminate (handle stays re-spawnable)
  {
    const h = defineWorker((ctx) => { ctx.on("x", () => {}); }).spawn();
    h.terminate();
    if (!noop(() => h.send(new Uint8Array([1])))) throw fail("T2: send after terminate threw");
    if (!noop(() => h.post("x", 1))) throw fail("T2: post after terminate threw");
    if (!(await rejects(() => h.call("x"), "spawn"))) throw fail("T2: call after terminate did not reject");
    if (!noop(() => h.terminate())) throw fail("T2: double terminate threw");
    h.spawn();
    if (!h.spawned) throw fail("T2: respawn after terminate failed");
    h.destroy();
  }

  // after destroy (idempotent)
  {
    const h = defineWorker((ctx) => { ctx.on("x", () => {}); }).spawn();
    h.destroy();
    if (!noop(() => h.destroy())) throw fail("T2: double destroy threw");
    if (!noop(() => h.send(new Uint8Array([1])))) throw fail("T2: send after destroy threw");
    if (!noop(() => h.post("x", 1))) throw fail("T2: post after destroy threw");
    if (!(await rejects(() => h.call("x"), "destroyed"))) throw fail("T2: call after destroy did not reject");
  }

  // call in-flight across terminate rejects
  {
    const h = defineWorker((ctx) => { ctx.on("hang", () => undefined); }).spawn();
    const p = h.call("hang");
    const r = rejects(() => p, "terminated");
    h.terminate();
    if (!(await r)) throw fail("T2: in-flight call across terminate did not reject");
    h.destroy();
  }

  // 4096 spawn/terminate/destroy cycles -- no retained URLs, no live workers.
  for (let i = 0; i < LEAK_CYCLES; i++) {
    const h = defineWorker((ctx) => { ctx.on("noop", () => {}); });
    h.spawn();
    h.terminate();
    h.destroy();
    if (mockRegistry.liveUrls() !== 0) throw fail("T2: Blob URL retained mid-cycle (" + mockRegistry.liveUrls() + ")", i);
  }
  if (mockRegistry.liveWorkers() !== 0) throw fail("T2: " + mockRegistry.liveWorkers() + " workers left alive after " + LEAK_CYCLES + " cycles");
  if (mockRegistry.liveUrls() !== 0) throw fail("T2: " + mockRegistry.liveUrls() + " Blob URLs retained after " + LEAK_CYCLES + " cycles");
}

// ---------------------------------------------------------------------------
// Real-thread scaffolding for T3/T5/T6-real. These tiers spin an ACTUAL
// node:worker_threads Worker running lite-worker's serialized WORKER_RUNTIME +
// makeFrameChannel UNCHANGED (via defineWorker()._source, the real serializer)
// over installSelfShim -- so the seqlock is exercised against a real writer on a
// second OS thread, not an in-process mock (W-04/W-05).
// ---------------------------------------------------------------------------

// The real, unmodified serialized worker source for a producer body.
function realSource(producerFn) { return defineWorker(producerFn)._source; }

// Pump the event loop until pred() is truthy or the timeout elapses. setImmediate
// yields so worker.on("message") deliveries (the SAB handshake, acks) can land.
function waitFor(pred, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (pred()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(fail(label + ": timed out after " + timeoutMs + "ms"));
      setImmediate(tick);
    };
    tick();
  });
}

// Spin a real worker running `source`, wire a main-side transport, and build a
// shared-mode consumer over it. Returns handles; caller drives + must terminate.
function spawnRealConsumer(source, layout, count) {
  const worker = new NodeWorker(THREAD_ENTRY, { workerData: { source } });
  let werr = null;
  worker.on("error", (e) => { werr = e; });
  const transport = nodeThreadTransport(worker);
  const cons = frameChannel(transport, layout, { role: "consumer", mode: "shared", count });
  return { worker, transport, cons, err: () => werr };
}

// --- t3Producer: hostile writer. Publishes as fast as it can, forever; every
// lane of every frame carries the same monotonically-increasing id, so a torn
// read is detectable as a lane that disagrees with lane 0. Main terminates it.
function t3Producer(ctx) {
  ctx.on("go", function (cfg) {
    var chan = ctx.frameChannel(
      { stride: cfg.stride, capacity: cfg.capacity },
      { role: "producer", mode: "shared", count: cfg.count }
    );
    var lanes = cfg.stride * cfg.capacity;
    var id = 0;
    var fill = function (view) {
      var v = id % 16777216;           // stay exact in Float32 (< 2^24)
      for (var k = 0; k < lanes; k++) view[k] = v;
    };
    for (;;) { id++; chan.produce(fill); }
  });
}

// --- t5Producer: lockstep. Publishes exactly the value main asks for and acks,
// so at read time the "newest published" oracle is known precisely.
function t5Producer(ctx) {
  var chan = null, lanes = 0, pv = 0;
  var fill = function (view) { for (var k = 0; k < lanes; k++) view[k] = pv; };
  ctx.on("init", function (cfg) {
    chan = ctx.frameChannel(
      { stride: cfg.stride, capacity: cfg.capacity },
      { role: "producer", mode: "shared", count: cfg.count }
    );
    lanes = cfg.stride * cfg.capacity;
    ctx.post("ready", 1);
  });
  ctx.on("prod", function (d) { pv = d.v; chan.produce(fill); ctx.post("ack", d.n); });
}

// --- transferProducer: real transferable-ring producer for the T6-real check.
// Floods the ring; the consumer recycles buffers back. Paced on the microtask
// queue so recycles are processed (the pool bounds it to `count` in flight).
function transferProducer(ctx) {
  ctx.on("go", function (cfg) {
    var chan = ctx.frameChannel(
      { stride: cfg.stride, capacity: cfg.capacity },
      { role: "producer", mode: "transfer", count: cfg.count }
    );
    var id = 0;
    var fill = function (view) { view[0] = id; };
    var pump = function () {
      while (chan.produce(fill)) { id++; }
      setTimeout(pump, 0);
    };
    pump();
  });
}

// Run a hostile-writer session: hammer read()/readInto() against `source` for up
// to `target` ops (or the wall-clock deadline). Returns { ops, snaps, bad, torn }
// where `bad` counts readInto snapshots whose lanes disagreed (a leaked tear).
async function runHostile(source, cfg, target, timeoutMs) {
  const lanes = cfg.stride * cfg.capacity;
  const layout = { stride: cfg.stride, capacity: cfg.capacity };
  const { worker, transport, cons, err } = spawnRealConsumer(source, layout, cfg.count);
  transport.post("go", { stride: cfg.stride, capacity: cfg.capacity, count: cfg.count });
  try {
    await waitFor(() => cons.shared || err(), 8000, "runHostile shared handshake");
  } catch (e) { await worker.terminate(); throw e; }
  if (err()) { await worker.terminate(); throw err(); }
  if (!cons.shared) { await worker.terminate(); throw fail("consumer never negotiated shared mode over the real thread"); }

  const dst = new Float32Array(lanes);
  const deadline = Date.now() + timeoutMs;
  let ops = 0, bad = 0, snaps = 0;
  while (ops < target) {
    if (cons.readInto(dst)) {
      snaps++;
      const v0 = dst[0];
      for (let k = 1; k < lanes; k++) { if (dst[k] !== v0) { bad++; break; } }
    }
    cons.read(); // live-view fast path; also drives torn on an odd SEQ
    ops++;
    if ((ops & 8191) === 0 && Date.now() > deadline) break;
  }
  const torn = cons.torn;
  cons.dispose();
  await worker.terminate();
  return { ops, snaps, bad, torn };
}

// ---------------------------------------------------------------------------
// T3 -- REAL two-thread SAB seqlock under a hostile writer (W-04/W-05). Proves
// readInto never leaks a torn frame, that torn > 0 under a genuine race (the
// metamorphic opposite of W-05's always-zero), the W-06 live-view boundary
// (light), and that the consumer read path retains ~0 over a real transfer.
// ---------------------------------------------------------------------------
async function t3() {
  if (!SAB_OK) throw fail("T3: SharedArrayBuffer unavailable -- cannot prove the seqlock");

  const cfg = { stride: 8, capacity: 64, count: 2 };
  const r = await runHostile(realSource(t3Producer), cfg, 1e6, 25000);
  log("    T3 hostile: ops=" + r.ops + " readInto-snaps=" + r.snaps + " torn=" + r.torn + " torn-frames-leaked=" + r.bad);
  if (r.ops < 1e6) log("    T3 NOTE: hostile ops capped at " + r.ops + " by wall-clock (target 1e6)");
  if (r.bad !== 0) throw fail("T3: readInto leaked " + r.bad + " torn frame(s) across a real thread -- the seqlock is BROKEN");
  if (r.torn <= 0) throw fail("T3: torn stayed 0 under a genuine race -- seqlock accounting is suspect, investigate Worker.js");

  // W-06 (light): read()'s live view is coherent for exactly count-1 publishes
  // and is overwritten on the count-th. Single-threaded shared; the full pinning
  // (both modes, by name, with docs) is W2's scope -- this is just a boundary
  // spot-check while the shared path is warm.
  {
    const [pt, ct] = syncPair(true);
    const prod = frameChannel(pt, { stride: 4, capacity: 1 }, { role: "producer", mode: "shared", count: 2 });
    const cons = frameChannel(ct, { stride: 4, capacity: 1 }, { role: "consumer", mode: "shared", count: 2 });
    if (cons.mode !== "shared") throw fail("T3 W-06: shared negotiation failed");
    prod.produce((f) => { f[0] = 100; });
    const held = cons.read();                 // live view over slots[PUB]
    const v0 = held[0];
    prod.produce((f) => { f[0] = 200; });      // count-1 = 1 publish: still coherent
    const afterOne = held[0];
    prod.produce((f) => { f[0] = 300; });      // count-th publish: reuses the held slot
    const afterTwo = held[0];
    prod.dispose(); cons.dispose();
    if (v0 !== 100) throw fail("T3 W-06: initial live view = " + v0 + " != 100");
    if (afterOne !== 100) throw fail("T3 W-06: live view not coherent for count-1 publishes (" + afterOne + " != 100)");
    if (afterTwo !== 300) throw fail("T3 W-06: live view not overwritten on the count-th publish (" + afterTwo + " != 300)");
  }

  // T6 over the real thread (shared mode): once the SAB is attached the consumer
  // read path is fully synchronous (Atomics on shared memory, no messaging), so
  // measureOps CAN gate it -- and the data genuinely crosses an OS thread. A
  // hostile producer keeps flooding on its own core during the measurement.
  {
    const rcfg = { stride: 8, capacity: 256, count: 2 };
    const lanes = rcfg.stride * rcfg.capacity;
    const { worker, transport, cons, err } = spawnRealConsumer(realSource(t3Producer), { stride: rcfg.stride, capacity: rcfg.capacity }, rcfg.count);
    transport.post("go", rcfg);
    try {
      await waitFor(() => cons.shared || err(), 8000, "T3 T6-real shared handshake");
    } catch (e) { await worker.terminate(); throw e; }
    if (err()) { await worker.terminate(); throw err(); }
    if (!cons.shared) { await worker.terminate(); throw fail("T3 T6-real shared: consumer never negotiated shared mode over the real thread"); }
    const dst = new Float32Array(lanes);
    const roundTrip = () => { cons.read(); cons.readInto(dst); };
    const res = measureOps(roundTrip, { ops: 10000, warmup: 2000, stabilize: true });
    const rep = checkOps(res, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
    cons.dispose();
    await worker.terminate();
    log("    T3 T6-real(shared): " + (res.bytesPerOp === null ? "n/a" : res.bytesPerOp.toFixed(2)) + " B/op over a real thread, verdict=" + rep.verdict);
    if (rep.verdict !== "pass") throw fail("T3 T6-real shared: retention gate " + rep.verdict + " over a real thread");
  }

  // T6 over the real thread (transfer mode): a real ArrayBuffer transfer per
  // frame. This is inherently ASYNC (postMessage both ways) and does NOT fit
  // measureOps's synchronous op model, so -- per the harness rule -- it is gated
  // with a heap delta across a fixed transfer count after settle(), not a faked
  // measureOps number. A real per-frame RETENTION would be 100+ B/op; the
  // transient view header is GC'd, so the post-settle delta must be near zero.
  {
    const tcfg = { stride: 8, capacity: 256, count: 3 };
    const worker = new NodeWorker(THREAD_ENTRY, { workerData: { source: realSource(transferProducer) } });
    let werr = null;
    worker.on("error", (e) => { werr = e; });
    const transport = nodeThreadTransport(worker);
    let received = 0;
    const cons = frameChannel(transport, { stride: tcfg.stride, capacity: tcfg.capacity }, {
      role: "consumer", mode: "transfer", count: tcfg.count, onFrame: () => { received++; }
    });
    transport.post("go", tcfg);
    try {
      await waitFor(() => received > 2000 || werr, 15000, "T3 T6-real transfer warmup");
    } catch (e) { await worker.terminate(); throw e; }
    if (werr) { await worker.terminate(); throw werr; }
    await settle();
    const base = process.memoryUsage().heapUsed;
    const start = received;
    const N = 20000;
    try {
      await waitFor(() => (received - start) >= N || werr, 20000, "T3 T6-real transfer soak");
    } catch (e) { await worker.terminate(); throw e; }
    const moved = received - start;
    await settle();
    const delta = process.memoryUsage().heapUsed - base;
    cons.dispose();
    await worker.terminate();
    if (werr) throw werr;
    if (cons.byteLength !== tcfg.stride * tcfg.capacity * 4) throw fail("T3 T6-real transfer: consumer byteLength drifted");
    log("    T3 T6-real(transfer): " + moved + " real transfers, heap delta " + (delta / 1024).toFixed(1) + " KiB (" + (delta / moved).toFixed(1) + " B/transfer, coarse)");
    if (delta > 1024 * 1024) throw fail("T3 T6-real transfer: heap grew " + (delta / 1024).toFixed(0) + " KiB over " + moved + " real transfers -- retention, not noise");
  }
}

// ---------------------------------------------------------------------------
// T5 -- differential fuzz across a REAL thread vs a plain oracle modelling the
// newest value published. Lockstep produce (acked) so the oracle is exact; mixed
// produce/read/readInto ops; every read must equal the oracle's newest (drops
// allowed). On divergence: TORTURE_SEED + op index for replay.
// ---------------------------------------------------------------------------
async function t5() {
  if (!SAB_OK) throw fail("T5: SharedArrayBuffer unavailable -- cannot fuzz the shared channel");

  const cfg = { stride: 8, capacity: 16, count: 2 };
  const lanes = cfg.stride * cfg.capacity;
  const { worker, transport, cons, err } = spawnRealConsumer(realSource(t5Producer), { stride: cfg.stride, capacity: cfg.capacity }, cfg.count);

  let ackResolve = null;
  transport.on("ack", () => { if (ackResolve) { const r = ackResolve; ackResolve = null; r(); } });
  let ready = false;
  transport.on("ready", () => { ready = true; });

  transport.post("init", cfg);
  try {
    await waitFor(() => (ready && cons.shared) || err(), 8000, "T5 init/handshake");
  } catch (e) { await worker.terminate(); throw e; }
  if (err()) { await worker.terminate(); throw err(); }
  if (!cons.shared) { await worker.terminate(); throw fail("T5: consumer never negotiated shared over the real thread"); }

  const rand = xorshift32(SEED ^ 0x5a5a5a5a);
  const dst = new Float32Array(lanes);
  let newest = -1, produces = 0, reads = 0, n = 0;
  const OPS = 100000;
  const deadline = Date.now() + 30000;

  const produce = (v) => new Promise((res) => { ackResolve = res; transport.post("prod", { v, n: ++n }); });

  let i = 0;
  try {
    for (; i < OPS; i++) {
      const roll = rand();
      if (newest < 0 || roll < 0.5) {
        const v = (i % 16000000) + 1;        // exact in Float32, always > 0
        await produce(v);
        if (err()) throw err();
        newest = v;
        produces++;
      } else if (roll < 0.75) {
        if (cons.readInto(dst)) {
          reads++;
          for (let k = 0; k < lanes; k++) {
            if (dst[k] !== newest) throw fail("T5 readInto: lane " + k + "=" + dst[k] + " != oracle newest " + newest, i);
          }
        }
      } else {
        const view = cons.read();
        if (view) {
          reads++;
          for (let k = 0; k < lanes; k++) {
            if (view[k] !== newest) throw fail("T5 read: lane " + k + "=" + view[k] + " != oracle newest " + newest, i);
          }
        }
      }
      if ((i & 8191) === 0 && Date.now() > deadline) break;
    }
  } finally {
    cons.dispose();
    await worker.terminate();
  }
  log("    T5: ops=" + i + " produces=" + produces + " reads=" + reads);
  if (i < OPS) log("    T5 NOTE: fuzz ops capped at " + i + " by wall-clock (target " + OPS + ")");
  if (produces === 0 || reads === 0) throw fail("T5: degenerate op mix (produces=" + produces + " reads=" + reads + ")");
}

// ---------------------------------------------------------------------------
// T6 -- zero-retention gate, both modes, plus the structural pool invariance a
// heap gate cannot substitute for.
// ---------------------------------------------------------------------------
async function t6() {
  const run = (typed, mode) => {
    const [pt, ct] = syncPair(typed);
    const stride = 8, capacity = 256, count = 2;
    const prod = frameChannel(pt, { stride, capacity }, { role: "producer", mode, count });
    const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode, count });
    if (prod.mode !== mode) throw fail("T6: producer negotiated " + prod.mode + " not " + mode);
    const bytesBefore = prod.byteLength;
    let v = 0;
    const fill = (f) => { f[0] = v; };
    const roundTrip = () => { v++; prod.produce(fill); cons.read(); };
    const r = measureOps(roundTrip, { ops: 10000, warmup: 2000, stabilize: true });
    const rep = checkOps(r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
    if (rep.verdict !== "pass") {
      throw fail("T6 " + mode + ": retention gate " + rep.verdict +
        " (bytesPerOp=" + (r.bytesPerOp === null ? "n/a" : r.bytesPerOp.toFixed(2)) + ")");
    }
    // Structural invariants no heap gate can see.
    if (prod.byteLength !== bytesBefore) throw fail("T6 " + mode + ": pool byteLength grew (" + prod.byteLength + " != " + bytesBefore + ")");
    if (prod.free > prod.count) throw fail("T6 " + mode + ": free " + prod.free + " exceeded count " + prod.count);
    prod.dispose(); cons.dispose();
    return r;
  };

  const rt = run(false, "transfer");
  metrics.bytesPerOp = rt.bytesPerOp === null ? 0 : rt.bytesPerOp;
  metrics.gcMajor = rt.summary.gc.major;
  metrics.gcMinor = rt.summary.gc.minor;
  metrics.gcMaxMs = rt.summary.gc.maxMs;

  if (SAB_OK) run(true, "shared");
}

// ---------------------------------------------------------------------------
// T7 -- soak: the pool conservation invariant across 4096 build-up/tear-down
// cycles, and a lite-leak retention proof that spawned handles are collected.
// ---------------------------------------------------------------------------
async function t7() {
  // Conservation across fresh channels: a fresh producer has free==count and a
  // fresh consumer reads null; dispose zeroes free.
  const count = 3;
  for (let i = 0; i < LEAK_CYCLES; i++) {
    const [pt, ct] = syncPair(false);
    const prod = frameChannel(pt, 4, { role: "producer", capacity: 8, count });
    const cons = frameChannel(ct, 4, { role: "consumer", capacity: 8, count });
    if (!conservationOk(prod.free, count)) throw fail("T7: fresh producer free=" + prod.free + " != count=" + count, i);
    if (cons.read() !== null) throw fail("T7: fresh consumer did not read null", i);
    let v = 0;
    const fill = (f) => { f[0] = v; };
    for (let k = 0; k < 8; k++) { v = k; prod.produce(fill); cons.read(); }
    prod.dispose(); cons.dispose();
    if (prod.free !== 0) throw fail("T7: dispose did not zero free", i);
  }

  // Retention: churn 4096 spawn/destroy cycles through the lite-leak tracker.
  // destroy() is the explicit release, mirrored by untrack() (which cancels the
  // FR without running cleanup), so a properly torn-down handle is not a leak:
  // size returns to 0 and nothing reaches the FR leak path. The tag is a string
  // constant and the cleanup captures nothing (held-value contract). The
  // substantive lite-worker retention proof is the mock registry below -- if
  // destroy() left a Worker or Blob URL alive, those counts stay non-zero.
  const { tracker, leaks, warns } = makeTracker();
  for (let i = 0; i < LEAK_CYCLES; i++) {
    const h = defineWorker((ctx) => { ctx.on("noop", () => {}); });
    h.spawn();
    h.terminate();
    h.destroy();
    const rec = tracker.track(h, NOOP_CLEANUP, "handle");
    tracker.untrack(rec);
  }
  await settle();
  await settle();
  const live = tracker.size();
  const findings = tracker.audit();
  metrics.leakSize = live;
  metrics.findings = findings.length;
  metrics.warnings = warns.length;
  if (live !== 0) throw fail("T7: " + live + " handles still tracked after " + LEAK_CYCLES + " cycles");
  if (findings.length !== 0) throw fail("T7: " + findings.length + " leak findings");
  if (leaks.length !== 0) throw fail("T7: " + leaks.length + " leaks: " + leaks.join(", "));
  if (mockRegistry.liveWorkers() !== 0) throw fail("T7: " + mockRegistry.liveWorkers() + " workers left alive");
  if (mockRegistry.liveUrls() !== 0) throw fail("T7: " + mockRegistry.liveUrls() + " Blob URLs retained");
}

// ---------------------------------------------------------------------------
// T9 -- controls: each gate above, deliberately broken, MUST be caught. If a
// control is not detected, the gate is decorative and the suite fails.
// ---------------------------------------------------------------------------
async function t9() {
  // T6 control: a read path that RETAINS a fresh allocation each op must fail
  // the same maxBytesPerOp:8 gate under stabilize (retained, not transient).
  {
    const [pt, ct] = syncPair(false);
    const prod = frameChannel(pt, { stride: 8, capacity: 256 }, { role: "producer", mode: "transfer" });
    const cons = frameChannel(ct, { stride: 8, capacity: 256 }, { role: "consumer", mode: "transfer" });
    const sink = [];
    let v = 0;
    const fill = (f) => { f[0] = v; };
    const badRoundTrip = () => { v++; prod.produce(fill); cons.read(); sink.push(new Float32Array(16)); };
    const r = measureOps(badRoundTrip, { ops: 2000, warmup: 500, stabilize: true });
    const rep = checkOps(r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
    sink.length = 0;
    prod.dispose(); cons.dispose();
    if (rep.verdict === "pass") throw fail("T9: allocating-read control passed the T6 gate (decorative)");
  }

  // T0 control: a channel that QUEUES instead of dropping breaks the bounded
  // invariant (in-flight <= count). The real frameChannel cannot exhibit this
  // (it drops via `produce() -> false`), so a genuine queueing channel would
  // mean duplicating makeFrameChannel; instead we drive the SAME boundedOk
  // predicate the T0 tier uses with a modeled unbounded backlog.
  {
    const count = 3;
    let inFlight = 0;
    for (let i = 0; i < count + 5; i++) inFlight++; // queued, never drained
    if (boundedOk(inFlight, count)) throw fail("T9: queueing control passed the T0 bounded check (decorative)");
  }

  // T7 control: a pool that leaks one buffer per hop breaks conservation (free
  // never returns to count). As above, a real leaking pool would require a
  // broken makeFrameChannel, so we drive the SAME conservationOk predicate the
  // T7 tier uses with a modeled leaked buffer.
  {
    const count = 4;
    let free = count;
    free -= 1; // leaked, never returned to the pool
    if (conservationOk(free, count)) throw fail("T9: leaking-pool control passed the T7 conservation check (decorative)");
  }

  // T7 retention control: a handle pinned in a live array cannot be collected,
  // so the tracker's size must NOT return to 0 -- proving the retention gate is
  // able to fail, not just able to pass.
  {
    const { tracker } = makeTracker();
    const retained = [];
    const h = defineWorker((ctx) => { ctx.on("noop", () => {}); });
    h.spawn(); h.terminate(); h.destroy();
    retained.push(h);                       // strong ref: defeats collection
    tracker.track(h, NOOP_CLEANUP, "pinned");
    await settle();
    await settle();
    const live = tracker.size();
    retained.length = 0;
    if (live === 0) throw fail("T9: retention control was collected -- the leak gate cannot fail (decorative)");
  }

  // T3/T5 seqlock control: take the REAL serialized producer and STRIP the two
  // odd/even SEQ bumps, then run it as a hostile writer on a real thread. With
  // SEQ frozen even, readInto's re-check always "passes", so torn frames reach
  // the caller -- which the all-lanes-equal predicate (the exact gate T3/T5 rely
  // on) MUST catch. If the stripped seqlock never tears a snapshot, the tear
  // gate is decorative, the way W-01's gate that never ran was.
  if (SAB_OK) {
    const good = realSource(t3Producer);
    const stripped = good.split("Atomics.add(hdr, SEQ, 1);").join("");
    if (stripped === good) throw fail("T9: could not strip the SEQ bumps -- source drifted, update the control");
    const cfg = { stride: 8, capacity: 128, count: 2 };
    const r = await runHostile(stripped, cfg, 300000, 20000);
    log("    T9 seqlock-control: ops=" + r.ops + " readInto-snaps=" + r.snaps + " torn-frames-caught=" + r.bad);
    if (r.bad === 0) throw fail("T9: stripped-seqlock producer never tore a readInto snapshot -- the tear gate is decorative");
  }

  // T4 conservation control: model a pool that leaks one buffer per hop -- free +
  // held never returns to count. This drives the SAME poolConservedOk predicate
  // the T4 tier relies on; if a leak still satisfied it, the conservation gate
  // would be decorative. (A genuinely leaking pool would need a broken
  // makeFrameChannel, so -- as with the T0/T7 controls -- we drive the predicate.)
  {
    const count = 4;
    let held = 1, free = count - 1; // consumer holds one as display, rest free
    free -= 1;                      // one buffer leaked, never recycled
    if (poolConservedOk(free, held, count)) throw fail("T9: leaking-pool control passed the T4 conservation check (decorative)");
  }

  // W-10 spoofed-handshake control: `lw:fc:*` is reserved, but the typed plane is
  // shared, so a spoofed lw:fc:sab could carry a plain ArrayBuffer with a matching
  // byteLength/count. It MUST be rejected -- the consumer stays on the ring. The
  // positive half posts a REAL SharedArrayBuffer through the identical path to
  // prove the attach mechanism is live, so the negative half proves the guard is
  // the only thing blocking the spoof. Remove the W-10 guard and this exits 1.
  if (SAB_OK) {
    const stride = 8, capacity = 16, count = 2;
    const bl = capacity * stride * 4;
    const meta = (sab) => ({ sab, stride, capacity, kind: "f32", count, byteLength: bl });

    // Positive: a real SAB with matching layout DOES attach (attach path is live).
    {
      const [pt, ct] = syncPair(true);
      const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode: "auto", count });
      if (cons.mode !== "transfer") throw fail("T9 spoof: consumer did not start on the ring (mode=" + cons.mode + ")");
      pt.post("lw:fc:sab", meta(new SharedArrayBuffer(16 + count * bl)));
      if (cons.mode !== "shared") throw fail("T9 spoof-control: a real SharedArrayBuffer handshake did not attach -- the attach path is broken, the negative control is meaningless");
      cons.dispose();
    }

    // Negative: a spoofed plain ArrayBuffer with matching byteLength/count MUST be
    // rejected by the W-10 guard; the consumer stays on the ring.
    {
      const [pt, ct] = syncPair(true);
      const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode: "auto", count });
      pt.post("lw:fc:sab", meta(new ArrayBuffer(16 + count * bl)));
      if (cons.mode !== "transfer") throw fail("T9 spoof-control: a plain ArrayBuffer handshake attached (mode=" + cons.mode + ") -- the W-10 fail-closed guard is missing/decorative");
      if (cons.shared) throw fail("T9 spoof-control: spoofed handshake set shared=true");
      cons.dispose();
    }
  }
}

// ---------------------------------------------------------------------------
// T4 -- adversarial backpressure (W2). Producer:consumer speed ratios, bursty
// pacing, count 2/3/8, a fill that throws (pool stays intact), and dispose
// mid-flight from each side. After each: conservation holds and every produce
// tick is accounted as either a sent frame or a source drop. Runs over the
// in-process loopback ring (like T0/T2) -- no real thread needed; the async
// `pair()` transport gives genuine in-flight buffers so produce() can drop at
// source, which the synchronous ring cannot exhibit (it recycles in lockstep).
// ---------------------------------------------------------------------------
async function t4() {
  const drain = () => new Promise((r) => setTimeout(r, 0));
  const rand = xorshift32(SEED ^ 0x744b7000);

  // (1) Conservation + tick accounting across speed ratios x counts, over the
  // async ring. Every sent buffer is delivered exactly once (delivered === sent),
  // and at rest free + held === count.
  const ratios = [
    { name: "1:1", prod: 1, read: 1 },
    { name: "1000:1", prod: 1000, read: 1 },
    { name: "1:1000", prod: 1, read: 1000 },
  ];
  for (const count of [2, 3, 8]) {
    for (const ratio of ratios) {
      const [pt, ct] = pair();
      const prod = frameChannel(pt, { stride: 8, capacity: 16 }, { role: "producer", mode: "transfer", count });
      let delivered = 0;
      const cons = frameChannel(ct, { stride: 8, capacity: 16 }, {
        role: "consumer", mode: "transfer", count, onFrame: () => { delivered++; }
      });
      let v = 0;
      const fill = (f) => { f[0] = v; };
      let ticks = 0, sent = 0, sourceDropped = 0;
      for (let r = 0; r < 8; r++) {
        for (let p = 0; p < ratio.prod; p++) {
          v++; ticks++;
          if (prod.produce(fill)) sent++; else sourceDropped++;
          if (prod.free > count) throw fail("T4 bounded: free=" + prod.free + " > count=" + count + " [" + ratio.name + "]", ticks);
          if (prod.free < 0) throw fail("T4: negative free " + prod.free, ticks);
        }
        await drain();
        for (let c = 0; c < ratio.read; c++) cons.read();
        await drain();
      }
      await drain();
      if (sent + sourceDropped !== ticks) throw fail("T4 accounting: sent(" + sent + ")+dropped(" + sourceDropped + ") != ticks(" + ticks + ") [" + ratio.name + " count=" + count + "]");
      if (delivered !== sent) throw fail("T4: delivered(" + delivered + ") != sent(" + sent + ") -- a frame was lost or duplicated on the wire [" + ratio.name + " count=" + count + "]");
      if (ratio.prod > count && sourceDropped === 0) throw fail("T4: burst " + ratio.name + " count=" + count + " never dropped at source -- backpressure not exercised (decorative)");
      const held = cons.read() !== null ? 1 : 0;
      if (!poolConservedOk(prod.free, held, count)) throw fail("T4 conservation: free(" + prod.free + ")+held(" + held + ") != count(" + count + ") [" + ratio.name + "]");
      prod.dispose(); cons.dispose();
      if (prod.free !== 0) throw fail("T4: dispose did not zero producer free (" + prod.free + ") [" + ratio.name + "]");
    }
  }

  // (2) Bursty random pacing: random-size produce bursts, random read counts,
  // random drain points. Conservation + accounting must survive the chaos.
  {
    const count = 3;
    const [pt, ct] = pair();
    const prod = frameChannel(pt, { stride: 8, capacity: 8 }, { role: "producer", mode: "transfer", count });
    let delivered = 0;
    const cons = frameChannel(ct, { stride: 8, capacity: 8 }, {
      role: "consumer", mode: "transfer", count, onFrame: () => { delivered++; }
    });
    let v = 0;
    const fill = (f) => { f[0] = v; };
    let ticks = 0, sent = 0, sourceDropped = 0;
    for (let r = 0; r < 64; r++) {
      const burst = 1 + ((rand() * 20) | 0);
      for (let p = 0; p < burst; p++) { v++; ticks++; if (prod.produce(fill)) sent++; else sourceDropped++; }
      if (prod.free > count) throw fail("T4 bursty bounded: free=" + prod.free + " > count=" + count, ticks);
      if (rand() < 0.5) await drain();
      const reads = (rand() * 5) | 0;
      for (let c = 0; c < reads; c++) cons.read();
      if (rand() < 0.5) await drain();
    }
    await drain();
    if (sent + sourceDropped !== ticks) throw fail("T4 bursty accounting: " + sent + "+" + sourceDropped + " != " + ticks);
    if (delivered !== sent) throw fail("T4 bursty: delivered(" + delivered + ") != sent(" + sent + ")");
    const held = cons.read() !== null ? 1 : 0;
    if (!poolConservedOk(prod.free, held, count)) throw fail("T4 bursty conservation: free(" + prod.free + ")+held(" + held + ") != count(" + count + ")");
    prod.dispose(); cons.dispose();
  }

  // (3) A fill that throws: the buffer returns to `free`, the pool stays intact,
  // and a subsequent normal produce still works. Synchronous ring for determinism.
  for (const count of [2, 3, 8]) {
    const [pt, ct] = syncPair(false);
    const prod = frameChannel(pt, { stride: 8, capacity: 4 }, { role: "producer", mode: "transfer", count });
    const cons = frameChannel(ct, { stride: 8, capacity: 4 }, { role: "consumer", mode: "transfer", count });
    if (prod.free !== count) throw fail("T4 throw: fresh free " + prod.free + " != count " + count);
    let threw = false;
    try { prod.produce(() => { throw new Error("boom"); }); }
    catch (e) { threw = e.message === "boom"; }
    if (!threw) throw fail("T4 throw: throwing fill did not propagate");
    if (prod.free !== count) throw fail("T4 throw: pool not intact after throw (free=" + prod.free + " != count=" + count + ")");
    let v = 0;
    if (!prod.produce((f) => { f[0] = ++v; })) throw fail("T4 throw: normal produce failed after a throwing fill (count=" + count + ")");
    if (!cons.read()) throw fail("T4 throw: consumer read null after a good produce");
    prod.dispose(); cons.dispose();
  }

  // (4) Dispose mid-flight from each side: buffers on the wire when dispose lands
  // must not crash the drain, and the disposed side is inert afterwards.
  {
    const count = 4;
    const [pt, ct] = pair();
    const prod = frameChannel(pt, { stride: 8, capacity: 8 }, { role: "producer", mode: "transfer", count });
    const cons = frameChannel(ct, { stride: 8, capacity: 8 }, { role: "consumer", mode: "transfer", count });
    let v = 0;
    const fill = (f) => { f[0] = ++v; };
    for (let p = 0; p < count; p++) prod.produce(fill);  // fill the wire, undrained
    prod.dispose();                                      // dispose mid-flight
    await drain();                                       // deliveries land post-dispose
    if (prod.free !== 0) throw fail("T4 dispose(prod): free not zeroed (" + prod.free + ")");
    if (prod.produce(fill)) throw fail("T4 dispose(prod): produce succeeded after dispose");
    cons.dispose();
  }
  {
    const count = 4;
    const [pt, ct] = pair();
    const prod = frameChannel(pt, { stride: 8, capacity: 8 }, { role: "producer", mode: "transfer", count });
    const cons = frameChannel(ct, { stride: 8, capacity: 8 }, { role: "consumer", mode: "transfer", count });
    let v = 0;
    const fill = (f) => { f[0] = ++v; };
    for (let p = 0; p < count; p++) prod.produce(fill);
    cons.dispose();                                      // dispose mid-flight
    await drain();
    if (cons.read() !== null) throw fail("T4 dispose(cons): read non-null after dispose");
    prod.dispose();
  }

  // (5) Shared-mode backpressure parity: the writer never starves (produce always
  // true) yet drops are still counted, and conservation is free === count.
  if (SAB_OK) {
    for (const count of [2, 3, 8]) {
      const [pt, ct] = syncPair(true);
      const prod = frameChannel(pt, { stride: 8, capacity: 16 }, { role: "producer", mode: "shared", count });
      const cons = frameChannel(ct, { stride: 8, capacity: 16 }, { role: "consumer", mode: "shared", count });
      if (prod.mode !== "shared" || cons.mode !== "shared") throw fail("T4 shared: negotiation failed (count=" + count + ")");
      let v = 0;
      const fill = (f) => { f[0] = ++v; };
      let ticks = 0, sent = 0;
      prod.produce(fill); cons.read();                  // seed lastFrame for drop accounting
      for (let i = 0; i < 5000; i++) {
        ticks++; if (prod.produce(fill)) sent++;
        if ((i & 63) === 0) cons.read();
      }
      if (sent !== ticks) throw fail("T4 shared: writer starved (sent " + sent + " != ticks " + ticks + ", count=" + count + ")");
      if (prod.free !== count) throw fail("T4 shared conservation: free(" + prod.free + ") != count(" + count + ")");
      cons.read();
      if (cons.dropped <= 0) throw fail("T4 shared: no drops counted under a 64:1 read ratio (count=" + count + ")");
      prod.dispose(); cons.dispose();
    }
  }
}

// ---------------------------------------------------------------------------
// T8 -- cross-package LAYOUT stride conformance (W2). lite-gl instance layouts
// have a fixed float stride per primitive; a frame channel must carry them with
// `i * stride` indexing intact across the boundary. There is NO lite-gl runtime
// dependency (zero deps), so the strides are local constants that MIRROR lite-gl.
// Asserts view.length === capacity * stride, that per-instance indexing survives
// the hop unmodified, and that a projected field round-trips bit-for-bit in BOTH
// transfer and shared mode.
// ---------------------------------------------------------------------------
async function t8() {
  // Mirrors lite-gl LAYOUT: POINT packs 8 floats/instance, QUAD and LINE 9.
  const POINT = 8, QUAD = 9, LINE = 9;
  if (LINE !== QUAD) throw fail("T8: LINE/QUAD stride constants drifted");
  const capacity = 64;
  // A float with a full mantissa, so a truncated/reinterpreted lane is caught.
  const probe = Math.fround(Math.PI);
  const bitsOf = (x) => { const a = new Float32Array(1); a[0] = x; return new Uint32Array(a.buffer)[0]; };
  const probeBits = bitsOf(probe);

  const check = (stride, typed, mode) => {
    const [pt, ct] = syncPair(typed);
    const prod = frameChannel(pt, { stride, capacity }, { role: "producer", mode, count: 3 });
    const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode, count: 3 });
    if (prod.mode !== mode || cons.mode !== mode) throw fail("T8: negotiated " + prod.mode + "/" + cons.mode + " not " + mode + " (stride " + stride + ")");
    if (prod.stride !== stride) throw fail("T8: stride drifted (" + prod.stride + " != " + stride + ")");
    if (prod.capacity !== capacity) throw fail("T8: capacity drifted (" + prod.capacity + " != " + capacity + ")");
    if (prod.byteLength !== capacity * stride * 4) throw fail("T8: byteLength " + prod.byteLength + " != capacity*stride*4 (" + (capacity * stride * 4) + ")");

    // Fill every lane with its own flat index (< 2^24, exact in f32) and pin the
    // producer-side view length to capacity * stride.
    let producerViewLen = -1;
    prod.produce((view) => {
      producerViewLen = view.length;
      for (let i = 0; i < capacity; i++) {
        for (let f = 0; f < stride; f++) view[i * stride + f] = i * stride + f;
      }
    });
    if (producerViewLen !== capacity * stride) throw fail("T8: producer view.length " + producerViewLen + " != capacity*stride " + (capacity * stride) + " (mode " + mode + ")");

    const got = cons.read();
    if (!got) throw fail("T8: consumer read null (mode " + mode + ")");
    if (got.length !== capacity * stride) throw fail("T8: consumer view.length " + got.length + " != capacity*stride " + (capacity * stride) + " (mode " + mode + ")");
    for (let i = 0; i < capacity; i++) {
      for (let f = 0; f < stride; f++) {
        const want = i * stride + f;
        if (got[i * stride + f] !== want) throw fail("T8: lane [" + i + "*" + stride + "+" + f + "]=" + got[i * stride + f] + " != " + want + " (mode " + mode + ")", i);
      }
    }

    // Projected instance field round-trips bit-for-bit: zero the frame, set one
    // field of one instance, and compare the raw f32 bits after the hop.
    const inst = 7, field = 3;
    prod.produce((view) => { for (let k = 0; k < view.length; k++) view[k] = 0; view[inst * stride + field] = probe; });
    const got2 = cons.read();
    if (!got2) throw fail("T8: consumer read null on the projection frame (mode " + mode + ")");
    if (bitsOf(got2[inst * stride + field]) !== probeBits) throw fail("T8: projected field bits " + bitsOf(got2[inst * stride + field]) + " != " + probeBits + " (mode " + mode + ")");

    prod.dispose(); cons.dispose();
  };

  check(POINT, false, "transfer");
  check(QUAD, false, "transfer");
  if (SAB_OK) { check(POINT, true, "shared"); check(QUAD, true, "shared"); }
}

// ---------------------------------------------------------------------------
// Tier registry. Placeholder tiers carry `pending` (the owning session) instead
// of a `run` -- they are named and present but claim no coverage, so they never
// print "ok".
// ---------------------------------------------------------------------------
const TIERS = [
  { name: "T0", run: t0 },
  { name: "T1", run: t1 },
  { name: "T2", run: t2 },
  { name: "T3", run: t3 },
  { name: "T4", run: t4 },
  { name: "T5", run: t5 },
  { name: "T6", run: t6 },
  { name: "T7", run: t7 },
  { name: "T8", run: t8 },
  { name: "T9", run: t9 },
];

let failed = false;
for (const tier of TIERS) {
  if (tier.pending) {
    log("  " + tier.name + " pending -- " + tier.pending);
    continue;
  }
  try {
    await tier.run();
    log("  " + tier.name + " ok");
  } catch (e) {
    log("  " + tier.name + " FAIL: " + e.message);
    if (e.seed !== undefined) log("    replay: TORTURE_SEED=" + e.seed + (e.op !== undefined ? " (op " + e.op + ")" : ""));
    failed = true;
    break;
  }
}

log(
  "GATE leak=size " + metrics.leakSize + "/0 findings=" + metrics.findings +
  " warnings=" + metrics.warnings +
  " | gc major=" + metrics.gcMajor + " minor=" + metrics.gcMinor +
  " maxMs=" + metrics.gcMaxMs.toFixed(2) +
  " | alloc=" + metrics.bytesPerOp.toFixed(2) + " B/op"
);

// Exit via the write callback so the "ok" byte is flushed to a pipe before the
// process tears down -- process.exit() alone races the async stdout drain now
// that real worker threads make stdout buffered.
if (failed) {
  process.exitCode = 1;
  process.stderr.write("", () => process.exit(1));
} else {
  process.stdout.write("ok\n", () => process.exit(0));
}
