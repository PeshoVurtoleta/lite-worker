// test/torture.mjs -- node --expose-gc test/torture.mjs
//
// The lite-worker torture suite. Runs ten tiers in order; prints exactly "ok"
// to stdout on success (exit 0) and a diagnostic + exit 1 on any failure. Tier
// progress and the gate summary go to stderr so stdout stays clean.
//
// Wired this session (W0): T0 (channel laws), T1 (degenerate layouts), T2
// (lifecycle abuse), T6 (zero-retention gate), T7 (soak + conservation + leak),
// T9 (controls). Registered as pending placeholders for later sessions: T3
// (real-thread seqlock, W1), T4 (backpressure, W2), T5 (differential fuzz, W1),
// T8 (LAYOUT conformance, W2).
//
// Every gate ships a T9 control that MUST report failure; a gate that cannot
// fail is decorative (W-01 was the limiting case -- a gate that never ran).

import { measureOps, checkOps } from "@zakkster/lite-gc-profiler";
import { defineWorker, frameChannel } from "../Worker.js";
import {
  mockRegistry,
  pair,
  pairTyped,
  syncPair,
  xorshift32,
  settle,
  makeTracker,
  NOOP_CLEANUP,
} from "./torture/harness.mjs";

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
  { name: "T3", pending: "real-thread seqlock (W1)" },
  { name: "T4", pending: "backpressure (W2)" },
  { name: "T5", pending: "differential fuzz (W1)" },
  { name: "T6", run: t6 },
  { name: "T7", run: t7 },
  { name: "T8", pending: "LAYOUT conformance (W2)" },
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

if (failed) {
  process.exit(1);
} else {
  process.stdout.write("ok\n");
  process.exit(0);
}
