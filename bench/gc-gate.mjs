/**
 * Release gates for @zakkster/lite-worker, measured with @zakkster/lite-gc-profiler.
 *
 * Run:  node --expose-gc bench/gc-gate.mjs
 *
 * Gates (per the v1.3.0 roadmap):
 *   1. Steady-state zero-alloc soak on the frame channel — 10k frames, heap flat,
 *      BOTH directions (producer -> consumer and the recycle back).
 *   2. The same soak in shared (SAB) mode, where the floor should be a true zero:
 *      no transfer, no view headers, no postMessage.
 *   3. spawn/destroy loop leak check — no orphaned Blob URLs, listeners, or workers.
 *   4. Bundle guard — the shipped surface stays a single file within budget.
 *
 * `stabilize: true` forces a full GC at each steady-phase boundary, so
 * bytesPerOp reports SURVIVING allocation (retention), which is the number that
 * actually matters for a long-running frame loop.
 */
import { checkOps, measureOps } from "@zakkster/lite-gc-profiler";
import { readFileSync, statSync } from "node:fs";
import { frameChannel, defineWorker } from "../Worker.d.ts";

const FRAMES = 10000;
let failures = 0;
let transferRate = 0;

function gate(name, fn) {
  try { fn(); console.log(`  PASS  ${name}`); }
  catch (e) { failures++; console.log(`  FAIL  ${name}\n        ${e.message.split("\n")[0]}`); }
}

// Gate the SAME measurement that was printed. Running assertOps separately would
// re-measure, so the reported number and the gated number could disagree — at a
// ~1 B/op noise floor that is exactly how a gate becomes flaky.
function gateOps(name, result, rules) {
  const report = checkOps(result, rules);
  if (report.verdict === "pass") { console.log(`  PASS  ${name}`); return; }
  failures++;
  const why = report.verdict === "inconclusive"
    ? "inconclusive (metric unavailable on this source)"
    : report.violations.map((v) => `${v.metric} ${v.actual} > ${v.limit}`).join("; ");
  console.log(`  FAIL  ${name}\n        ${why}`);
}

// A synchronous transport pair: delivery is a direct call, so a "frame" is one
// unit of work with no event-loop noise in the measurement.
function syncPair(typed) {
  const mk = () => ({
    _r: new Set(), _t: new Map(), other: null,
    onRaw(fn) { this._r.add(fn); return () => this._r.delete(fn); },
    send(buf) { this.other._r.forEach((f) => f(buf)); },
    on(type, fn) { let s = this._t.get(type); if (!s) { s = new Set(); this._t.set(type, s); } s.add(fn); return () => s.delete(fn); },
    post(type, data) { const s = this.other._t.get(type); if (s) s.forEach((f) => f(data)); }
  });
  const a = mk(), b = mk(); a.other = b; b.other = a;
  return typed ? [a, b] : [{ onRaw: a.onRaw.bind(a), send: a.send.bind(a) }, { onRaw: b.onRaw.bind(b), send: b.send.bind(b) }];
}

console.log("lite-worker release gates\n");

// ---------------------------------------------------------------------------
// 1. Transferable ring: steady-state zero-alloc, both directions.
// ---------------------------------------------------------------------------
console.log("[1] frame channel — transferable ring, 10k frames, both directions");
{
  const [pt, ct] = syncPair(false);
  const stride = 8, capacity = 256;
  const prod = frameChannel(pt, { stride, capacity }, { role: "producer", mode: "transfer" });
  const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode: "transfer" });

  // One op = a full round trip: fill + publish (producer -> consumer) and the
  // read that lets the buffer recycle (consumer -> producer).
  const roundTrip = (i) => { prod.produce((f) => { f[0] = i; }); cons.read(); };

  const r = measureOps(roundTrip, { ops: FRAMES, warmup: 2000, stabilize: true });
  transferRate = r.opsPerSec;
  console.log(`      ${r.bytesPerOp === null ? "n/a" : r.bytesPerOp.toFixed(2)} B retained/frame · ${(r.opsPerSec / 1000).toFixed(0)}K frames/s · source=${r.source}`);
  gateOps("transferable ring retains < 8 B/frame in steady state", r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
  gate("pool never grows (free <= count after 10k frames)", () => {
    if (prod.free > prod.count) throw new Error(`free=${prod.free} > count=${prod.count}`);
  });
  prod.dispose(); cons.dispose();
}

// ---------------------------------------------------------------------------
// 2. Shared (SAB) mode: the floor should be a literal zero.
// ---------------------------------------------------------------------------
console.log("\n[2] frame channel — shared (SAB) mode, 10k frames");
if (typeof SharedArrayBuffer === "undefined") {
  console.log("      SKIP — SharedArrayBuffer unavailable in this runtime");
} else {
  const [pt, ct] = syncPair(true);
  const stride = 8, capacity = 256;
  const prod = frameChannel(pt, { stride, capacity }, { role: "producer", mode: "shared" });
  const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode: "shared" });
  if (cons.mode !== "shared") throw new Error("consumer failed to negotiate shared mode");

  const roundTrip = (i) => { prod.produce((f) => { f[0] = i; }); cons.read(); };
  const r = measureOps(roundTrip, { ops: FRAMES, warmup: 2000, stabilize: true });
  console.log(`      ${r.bytesPerOp === null ? "n/a" : r.bytesPerOp.toFixed(2)} B retained/frame · ${(r.opsPerSec / 1000).toFixed(0)}K frames/s · torn=${cons.torn}`);
  // 8 B/frame is the regression detector, not a claim of literal zero: a real
  // per-frame allocation (a fresh view, an envelope) costs 100+ B/op, while the
  // residual here sits at the ~1 B heap-sampling noise floor in BOTH modes. The
  // resolvable difference between the two is throughput, reported below.
  gateOps("shared mode retains < 8 B/frame in steady state", r, { maxBytesPerOp: 8, maxMajorsPerKOp: 0 });
  if (transferRate) {
    console.log(`      shared vs transferable: ${(r.opsPerSec / transferRate).toFixed(2)}x throughput, and zero postMessage traffic`);
  }
  gate("shared mode never tore a read under a synchronous producer", () => {
    if (cons.torn > 0) throw new Error(`torn=${cons.torn}`);
  });
  prod.dispose(); cons.dispose();
}

// ---------------------------------------------------------------------------
// 3. spawn/destroy leak gate — no orphaned Blob URLs, listeners, or workers.
// ---------------------------------------------------------------------------
console.log("\n[3] spawn/destroy loop — orphan check");
{
  const savedBlob = globalThis.Blob, savedURL = globalThis.URL, savedWorker = globalThis.Worker;
  const live = new Map(); let made = 0, revoked = 0, alive = 0;
  globalThis.Blob = class { constructor(p) { this._src = p.join(""); } };
  globalThis.URL = {
    createObjectURL(b) { const u = "blob:gate/" + ++made; live.set(u, b._src); return u; },
    revokeObjectURL(u) { if (live.delete(u)) revoked++; }
  };
  globalThis.Worker = class {
    constructor(url) { alive++; this.onmessage = null; const self = { onmessage: null, close() {}, postMessage() {} }; new Function("self", live.get(url) || "")(self); }
    postMessage() {} terminate() { alive--; }
  };

  for (let i = 0; i < 200; i++) {
    const h = defineWorker((ctx) => { ctx.on("noop", () => {}); }).spawn();
    h.terminate();
    h.destroy();
  }
  gate("no Blob URLs retained across 200 spawn/destroy cycles", () => {
    if (live.size !== 0) throw new Error(`${live.size} object URLs still live (made=${made}, revoked=${revoked})`);
  });
  gate("no worker threads left alive", () => {
    if (alive !== 0) throw new Error(`${alive} workers still alive`);
  });
  globalThis.Blob = savedBlob; globalThis.URL = savedURL; globalThis.Worker = savedWorker;
}

// ---------------------------------------------------------------------------
// 4. Bundle guard — the shipped surface stays one file, within budget.
// ---------------------------------------------------------------------------
console.log("\n[4] bundle guard");
{
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  const BUDGET_KB = 40;
  const bytes = statSync(new URL("../Worker.js", import.meta.url)).size;
  console.log(`      Worker.js = ${(bytes / 1024).toFixed(1)} KiB (budget ${BUDGET_KB} KiB) · files[] = ${pkg.files.join(", ")}`);
  gate(`Worker.js stays under ${BUDGET_KB} KiB`, () => {
    if (bytes > BUDGET_KB * 1024) throw new Error(`${(bytes / 1024).toFixed(1)} KiB exceeds budget`);
  });
  gate("exactly one JS file ships", () => {
    const js = pkg.files.filter((f) => f.endsWith(".js"));
    if (js.length !== 1) throw new Error(`files[] carries ${js.length} .js entries: ${js.join(", ")}`);
  });
  gate("no runtime dependencies", () => {
    const deps = Object.keys(pkg.dependencies || {});
    if (deps.length) throw new Error(`dependencies: ${deps.join(", ")}`);
  });
}

console.log(failures === 0 ? "\nAll gates passed." : `\n${failures} gate(s) FAILED.`);
process.exit(failures ? 1 : 0);
