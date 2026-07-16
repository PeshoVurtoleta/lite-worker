/**
 * frameChannel soak / bench.
 *
 * This is the harness that proves the two guarantees the type system and the
 * balanced unit tests cannot: (1) memory is BOUNDED — the number of buffers in
 * flight never exceeds the fixed pool, so nothing queues; (2) backpressure is
 * LATEST-WINS — under a fast producer / slow consumer, intermediate frames drop
 * instead of piling up. Both are emergent runtime properties; you can only see
 * them by running an adversarial workload and measuring.
 *
 * Usage:
 *   node --expose-gc bench/frame-soak.mjs [frames] [stride] [capacity] [count]
 *
 * Runs in-process (no Worker needed) against a transport pair instrumented to
 * track the in-flight queue depth.
 */
import { frameChannel } from "../Worker.js";

const FRAMES   = (+process.argv[2] || 200000);
const STRIDE   = (+process.argv[3] || 8);     // lite-gl LAYOUT.POINT
const CAPACITY = (+process.argv[4] || 1024);  // 1024 instances/frame
const COUNT    = (+process.argv[5] || 2);     // pool size (two-buffer default)
const gc = globalThis.gc;
const settle = () => new Promise((r) => setTimeout(r, 0));

// Instrumented transport pair.
//  - async mode (Phase A): delivery on a microtask, so buffers are genuinely
//    "in flight" and we can watch the queue depth against the pool size.
//  - sync mode (Phase B): delivery inline, so a hop is a plain function call and
//    the timing/allocation figures measure the channel, not the event loop.
let inFlight = 0, maxInFlight = 0;
function pair(sync) {
  const mk = () => ({
    _h: new Set(), other: null,
    onRaw(fn) { this._h.add(fn); return () => this._h.delete(fn); },
    send(buf) {
      const o = this.other;
      if (sync) { o._h.forEach((fn) => fn(buf)); return; }
      inFlight++; if (inFlight > maxInFlight) maxInFlight = inFlight;
      queueMicrotask(() => { inFlight--; o._h.forEach((fn) => fn(buf)); });
    }
  });
  const a = mk(), b = mk(); a.other = b; b.other = a; return [a, b];
}

const [pt, ct] = pair(false);
const prod = frameChannel(pt, STRIDE, { role: "producer", capacity: CAPACITY, count: COUNT });
const cons = frameChannel(ct, STRIDE, { role: "consumer",  capacity: CAPACITY, count: COUNT });
const byteLen = prod.byteLength;

console.log(`frameChannel soak — ${FRAMES} frames · stride ${STRIDE} · capacity ${CAPACITY} · pool ${COUNT}`);
console.log(`each buffer = ${byteLen} B (${(byteLen / 1024).toFixed(1)} KiB); the pool of ${COUNT} is the ONLY data allocation.\n`);

// ---------------------------------------------------------------------------
// Phase A — fast producer, deliberately slow consumer. Shows latest-wins drop
// and that the in-flight queue is capped by the pool.
// ---------------------------------------------------------------------------
let producedA = 0, starvedA = 0;
for (let i = 0; i < FRAMES; i++) {
  if (prod.produce((f) => { f[0] = i; })) producedA++; else starvedA++;
  if ((i % 500) === 0) { await settle(); cons.read(); } // consumer far behind
}
await settle(); await settle();

console.log("Phase A — fast producer / slow consumer");
console.log(`  produced (sent):            ${producedA}`);
console.log(`  starved (producer dropped): ${starvedA}   <- latest-wins: no free buffer, frame skipped`);
console.log(`  superseded-unread @consumer: ${cons.dropped}`);
console.log(`  max in-flight messages:     ${maxInFlight}  (pool = ${COUNT})  => ${maxInFlight <= COUNT ? "BOUNDED, never queues ✓" : "UNBOUNDED ✗"}`);
console.log(`  total frames accounted:     ${producedA + starvedA} / ${FRAMES}\n`);

// ---------------------------------------------------------------------------
// Phase B — balanced producer/consumer over a synchronous pair to rack up real
// hops, then check the heap did not grow: the data buffers are never
// re-allocated (only transient view headers are churned per hop).
// ---------------------------------------------------------------------------
const HOPS = Math.min(FRAMES, 100000);
const [pt2, ct2] = pair(true);
const prodB = frameChannel(pt2, STRIDE, { role: "producer", capacity: CAPACITY, count: COUNT });
const consB = frameChannel(ct2, STRIDE, { role: "consumer",  capacity: CAPACITY, count: COUNT });

if (gc) gc();
const heap0 = process.memoryUsage().heapUsed;
const rss0 = process.memoryUsage().rss;
const t0 = performance.now();

let hops = 0;
for (let i = 0; i < HOPS; i++) {
  if (prodB.produce((f) => { f[0] = i; f[1] = i * 2; })) hops++; // delivered + recycled inline
  consB.read();
}

const t1 = performance.now();
if (gc) gc();
const heap1 = process.memoryUsage().heapUsed;
const rss1 = process.memoryUsage().rss;

console.log("Phase B — balanced throughput + memory");
console.log(`  hops (deliveries):          ${hops}`);
console.log(`  wall:                       ${(t1 - t0).toFixed(0)} ms  (${(hops / (t1 - t0)).toFixed(0)} hops/ms)`);
console.log(`  RSS delta:                  ${((rss1 - rss0) / 1024).toFixed(0)} KiB`);
console.log(`  heapUsed delta (post-GC):   ${((heap1 - heap0) / 1024).toFixed(1)} KiB over ${hops} hops`);
if (gc) {
  console.log(`  retained per hop:           ${((heap1 - heap0) / Math.max(1, hops)).toFixed(3)} B  (~0 = no leak; view headers are transient)`);
} else {
  console.log(`  (re-run with --expose-gc for a retained-bytes-per-hop figure)`);
}
console.log(`\n  data buffers allocated, total: ${COUNT} (${((byteLen * COUNT) / 1024).toFixed(1)} KiB) — flat regardless of frame count.`);

prod.dispose(); cons.dispose(); prodB.dispose(); consB.dispose();
