# lite-worker research: lossless cross-thread queues over a SharedArrayBuffer

- **Status:** research record, not a plan. The plan is `ROADMAP.md` (track M);
  the settled design is `decisions/0002-mailbox.md` (2026-10-18). Notes
  marked "Settled 2026-10-18" are later additions; the rest is the record.
- **Date:** 2026-10-18.
- **Baseline:** lite-worker 1.4.0 (`7558bac`), lite-worker-pool 1.1.0 (`f53c98c`).
- **Sources:** `../Research/BITWISE_RESEARCH.md` section 2 (candidate A) and its
  prototypes in `../Research/bits-prototypes/` (`sab-queue.mjs`, `sab-alloc.mjs`,
  `pm-rate*.mjs`). Re-probed on 2026-10-18 (section 7).

---

## 0. Credit: Dmitry Vyukov's bounded MPMC queue

The core algorithm in this document is **Dmitry Vyukov's bounded
multi-producer / multi-consumer array queue**, which he published on his site
1024cores.net ("Bounded MPMC queue", in the lock-free algorithms / queues
section, circa 2010-2011). Everything below that is not credited to someone
else is either his design or a JavaScript-specific consequence of it.

What is Vyukov's, specifically:

- **The per-cell sequence number.** Each cell carries its own `seq`. A cell is
  free for a producer at position `pos` when `seq === pos`, holds a published
  value for a consumer at `pos` when `seq === pos + 1`, and is handed back for
  the next lap by writing `seq = pos + capacity`. This one idea replaces the
  separate "full" / "empty" flags or counters other designs need, and it is
  what makes the queue ABA-safe without tagged pointers.
- **One CAS per operation.** A producer claims a position with one CAS on
  `tail`; a consumer claims with one CAS on `head`. Data moves with plain
  stores, published by the `seq` store.
- **Fail-on-overflow, bounded, array-based.** No allocation after
  construction, no linked nodes, no memory reclamation problem.
- **His own honesty about progress.** Third-party documentation of his queue
  (libcds, which ports it as `VyukovMPMCCycleQueue`) records his
  characterisation that it is *not lock-free in the official meaning*, only
  built from atomic RMW operations without mutexes. Section 4.1 measures
  exactly that property in JavaScript.

License of the original code: the C++ listing is distributed by Vyukov under a
**Simplified BSD license**, Copyright (c) 2010-2011 Dmitry Vyukov (as
reproduced in libcds and in spdlog's modified copy). The JavaScript in this
package's prototypes is an independent re-implementation of the published
algorithm, not a transcription of his listing. **Before any code ships:**
re-read the original page and its code-license page at the source, and if any
shipped code is a line-by-line translation of his listing, reproduce his
notice in the source header and in a NOTICE section of the README. Either way,
the README, llms.txt, CHANGELOG entry and source header comment that introduce
a Vyukov-based structure must name him and link his page. On 2026-10-18 the
1024cores.net URL returned a redirect to a loopback address from this host, so
the license text above is taken from the mirrors listed in section 9; confirm
it at the source.

Related prior art (credited where used below):

- Leslie Lamport's single-producer / single-consumer ring (1983), the ancestor
  of every SPSC ring here: each side owns one index, no RMW at all.
- The LMAX Disruptor (Thompson, Farley, Barker, Gee, Stewart, 2011): a ring
  with a sequence per consumer; the same per-slot-sequence family as Vyukov's.
- Paul Adenot's `ringbuf.js`: the reference SPSC SharedArrayBuffer ring for the
  web (audio worklets). SPSC only.

---

## 1. Why lite-worker wants a lossless queue at all

lite-worker 1.4.0 has two cross-thread paths:

| Path | Semantics | Where |
|---|---|---|
| `frameChannel` | single producer, **latest-wins**: intermediate frames drop by design | `Worker.js:148-425`; shared mode (SAB + `Atomics` seqlock) since 1.3.0 |
| `post` / `call` / `on` | lossless, but every message is a `postMessage`: structured clone + `MessageEvent` + an event-loop task | `WorkerHandle` |

Latest-wins is right for rendering and wrong for streams where every item
matters: input / command events, job results, profiler samples. Those go
through `postMessage`, which allocates per message. lite-worker-pool measured
that cost on its own path at ~1.1 KB/job (Node 26) and ~1.4 KB/job (Node 22)
(`LiteWorkerPool/research/worker-set.md` E10) and names "a SharedArrayBuffer
ring transport" as the fix it has not built.

The win to claim is **zero allocation per message and batched draining**. Raw
throughput over `postMessage` is real but host-dependent (section 7).

---

## 2. The algorithm, in JavaScript

Layout: one `SharedArrayBuffer`, viewed as `Int32Array`s.

```text
header  : head, tail, each on its OWN 128-byte line (see 4.3)
seq     : Int32Array(CAP)          CAP = 2^k, CAP < 2^31
payload : Int32Array(CAP * W)      W int32 words per message (fixed)
init    : seq[i] = start + i  for the CAP positions starting at `start`
```

Settled 2026-10-18 (decisions/0002 M-03): the payload view is Int32Array or
Float64Array, chosen per lane, 8-byte aligned; SPSC lanes need no `seq` array.

Vyukov's protocol (producer side, multi-producer form):

```js
function enqueue(h, seq, dat, a, b) {
  let pos = Atomics.load(h, TAIL);
  for (;;) {
    const cell = pos & MASK;
    const dif = (Atomics.load(seq, cell) - pos) | 0;    // wrap-safe compare
    if (dif === 0) {                                    // cell free for this lap
      if (Atomics.compareExchange(h, TAIL, pos, (pos + 1) | 0) === pos) {
        dat[2 * cell] = a; dat[2 * cell + 1] = b;       // plain stores
        Atomics.store(seq, cell, (pos + 1) | 0);        // publish (the release)
        return true;
      }
      pos = Atomics.load(h, TAIL);                      // lost the race: retry
    } else if (dif < 0) {
      return false;                                     // FULL: a result, never a drop
    } else {
      pos = Atomics.load(h, TAIL);                      // another producer moved on
    }
  }
}
```

Consumer side, single-consumer form (no CAS on `head`):

```js
function dequeue(h, seq, dat, out) {
  const pos = Atomics.load(h, HEAD), cell = pos & MASK;
  if (((Atomics.load(seq, cell) - ((pos + 1) | 0)) | 0) !== 0) return false; // empty / unpublished
  out[0] = dat[2 * cell]; out[1] = dat[2 * cell + 1];   // read BEFORE handing back
  Atomics.store(seq, cell, (pos + CAP) | 0);            // hand the cell to the next lap
  Atomics.store(h, HEAD, (pos + 1) | 0);
  return true;
}
```

The multi-consumer form mirrors the producer: CAS `head` from `pos` to `pos+1`
when `seq - (pos+1) === 0`.

**Why it is correct in JS.** `Atomics` operations are sequentially consistent
in the ECMAScript memory model, and a plain write that precedes an
`Atomics.store` in one agent happens-before a plain read that follows an
`Atomics.load` observing that store in another agent. So the payload stores
are visible once the consumer sees `seq === pos + 1`.

**The wrap law.** Positions are int32 and wrap. That is safe if and only if
`CAP < 2^31` and every comparison is a difference, `(a - b) | 0`, never
`a < b`. Test it by **starting** positions near `2^31` (the prototype starts at
`2^31 - 5000`), so the wrap happens in the first milliseconds of every run,
with no hooks.

---

## 3. Best practice: choose the topology first

Vyukov's queue is the general answer. It is not always the right one. Pick
the weakest structure that covers the topology; each step down removes RMW
operations and failure modes.

| Topology | Use | Why |
|---|---|---|
| 1 producer -> 1 consumer | **SPSC ring** (Lamport; `ringbuf.js` shape) | No CAS at all. Each side owns one index. Wait-free. |
| N workers -> main (results, events, samples) | **One SPSC lane per producer**, drained by the consumer | No CAS, no contention, and a terminated worker only kills its own lane (4.1). Per-producer FIFO, which is all the oracle checks anyway. |
| Fixed batch of K items -> N workers (pool `map()`) | **An atomic ticket counter**: `i = Atomics.add(next, 0, 1)` over a shared items array | Not a queue at all. Wait-free. The items are known up front, so there is nothing to enqueue. |
| Streaming jobs -> any of N workers (pull mode), or N -> N | **Vyukov bounded MPMC** | This is the case his algorithm exists for: several claimants on the same end, unknown arrival times, bounded memory. |
| Targeted jobs -> worker i (per-key placement) | **SPSC lane per worker** | Work must not move between workers (lite-worker-pool `createWorkerSet` never steals, to keep per-key order). |

Rule of thumb: **one claimant per end = no CAS on that end.** Vyukov's design
degrades gracefully to that case (the single-consumer `dequeue` above), but
when *both* ends have one claimant, the SPSC ring is simpler still.

Settled 2026-10-18 (decisions/0002 M-02): rows 2 and 5 ship as SPSC lanes in
both directions for lite-worker-pool `createWorkerSet`; row 4 (Vyukov MPMC)
ships only for an opt-in pull-mode set.

---

## 4. Pitfalls (each one observed or measured)

### 4.1 A producer that dies between the claim and the publish wedges the queue

A producer CASes `tail` (the cell is now claimed) and is terminated before its
`Atomics.store(seq, ...)`. The consumer waits at that cell forever, since
`seq` never reaches `pos + 1`. Every message behind it is unreachable. Once the
ring wraps around to the dead cell, every other producer sees `dif < 0`
("full") forever.

Measured 2026-10-18 (probe in Appendix A): one healthy producer, one producer
that exits right after its claim. Result: **0 messages received; tail stuck at
1024, i.e. the ring full**. This is the "not lock-free in the official
meaning" property, observed in JS. In a worker system, termination is routine:
`terminate()`, crash, tab close, respawn. The same applies to a multi-consumer
dequeue: a consumer that dies after its `head` CAS and before handing the cell
back blocks producers one lap later.

Design consequences:

- **N -> 1: use per-producer SPSC lanes.** An SPSC producer has no claim step:
  it writes the payload, then stores `tail`. A producer that dies mid-write
  leaves an invisible partial message, and the lane can be reset by its
  consumer once the owner is known dead (lite-worker's handle knows).
- **A true MPMC queue needs a recovery story before it ships.** The honest one
  is coarse: on any participant's death, retire the whole queue (generation
  bump), rebuild it, and re-issue in-flight work from an owner table
  (`job -> worker`). Never try to "skip" a claimed cell: you cannot tell a dead
  claimant from a slow one without a timeout, and a timeout is a guess.
- Torture lane: kill a producer (and, for MPMC, a consumer) at every point
  between claim and publish; the recovery must lose nothing and duplicate
  nothing according to the oracle.

Settled 2026-10-18 (decisions/0002 M-09, M-10): a dead side's lanes are
retired and replaced (fresh SAB per generation), not reset in place; MPMC
execution is at-least-once, results exactly-once via job-id dedupe on main.

### 4.2 The browser main thread caps a per-frame drain at CAP x fps

`Atomics.wait` throws on the browser main thread, so a main-thread consumer
drains once per frame (or on `Atomics.waitAsync` where the engine supports it;
feature-detect). With `CAP = 1024` and 60 Hz, the sustained ceiling is about
61 k msgs/s, **below** `postMessage`. Size the ring as
`peak rate / drain rate x safety`, or add a waitAsync wake-up, and treat
"full" as a reported backpressure result.

### 4.3 False sharing: Apple Silicon has 128-byte lines

`head` and `tail` written by different threads must not share a line. The
prototype places them 16 int32 words (64 bytes) apart. On this host
`hw.cachelinesize = 128` (Apple M4 Pro), so they share a line. Intel's
adjacent-line prefetcher pairs 64-byte lines too. **Separate hot words by
128 bytes (32 int32 words) on every host**, and keep `seq` cells written by
different sides apart from the header.

### 4.4 A must-fail control that does not fire proves nothing

The racy control (claim with load + store instead of CAS) **did not fire** in
the bitmap allocator prototype when threads worked on separate words: 0
violations at 34-36 M ops/s. It fired (659 k to 1.1 M violations) only in a
contended lane where every thread hits word 0. Every torture battery here
needs a contended lane, and the racy control must fail *there*.

Settled 2026-10-18 (decisions/0002 M-12): this CAS control applies to the MPMC
claim and the slab bitmap only. An SPSC lane has no CAS; its controls are two
producers on one lane, publish-before-write, and single-thread wrap / FULL
mutants. A real-thread control fires probabilistically: M2 measures its rate
over N runs before it may gate in the DETERMINISTIC tier.

### 4.5 Read the payload before handing the cell back

`dequeue` must copy (or finish using) the payload before
`Atomics.store(seq, cell, pos + CAP)`. After that store a producer may
overwrite the cell. If the API hands the consumer a view of the payload, the
view is valid only inside the callback: the same live-view contract as
`frameChannel` (decisions/0001, W-06).

### 4.6 Keep positions inside the typed array

Read and write `head` / `tail` through the `Int32Array`, never cached in a JS
field across calls. Values from 2^30 up box in Chrome object fields with
pointer compression, and a cached copy is stale by definition.

### 4.7 `1 << 32 === 1`, and the other int32 traps

Masks are `CAP - 1` with `CAP` a power of two below 2^31. Shift counts are
taken mod 32. Use `(a - b) | 0` for every position comparison. The full
field guide is `../Research/BITWISE_RESEARCH.md` section 8.

### 4.8 Cross-origin isolation is a hard prerequisite

`SharedArrayBuffer` requires COOP/COEP in browsers. lite-worker already
negotiates this for `frameChannel` (`Worker.js:187-211`): `"auto"` falls back
to transfer, `"shared"` throws without isolation, and only a real SAB earns
the upgrade (`Worker.js:327-328`). A lossless channel must reuse that
negotiation, and its transfer fallback must keep the lossless contract
(postMessage is lossless, just slower and allocating).

### 4.9 Payload encoding can eat the win

The queue moves int32 words. If callers must encode objects into words per
message, that encoding cost (and any allocation in it) belongs in the
measurement. Fixed-width numeric records are the sweet spot; variable-size
payloads need a slab (the bitmap slot allocator in `sab-alloc.mjs`) or, later,
a TLSF heap per producer (`BITWISE_RESEARCH.md` section 4).

Settled 2026-10-18 (decisions/0002 M-03, M-06): lanes carry Int32 or Float64
words, chosen per lane, so f64 records need no encoding; variable sizes use the
bitmap slot slab, one per producer per lane; TLSF stays on hold.

---

## 5. How to measure correctly

### 5.1 Correctness is DETERMINISTIC-tier and gated on every run

- **Oracle:** every message carries `(producerId, counter)`. The consumer
  checks exactly-once, in-order delivery per producer, and counts missing ones.
- **Lanes:**
  1. uncontended;
  2. contended (all producers on one queue, tiny ring);
  3. wrap start at `2^31 - small`;
  4. slow consumer (FULL must be returned, never a silent drop or an unbounded
     buffer);
  5. producer termination between claim and publish (4.1), and consumer
     termination for MPMC;
  6. the **racy control**, which must fail in lane 2 (4.4).
- **Allocation:** 0 B/message on the steady-state path, via
  lite-gc-profiler / lite-leak under `node --expose-gc test/torture.mjs`. This
  is a count, not a timing, so it gates in `npm test`.

Settled 2026-10-18 (decisions/0002 M-12): the lanes cover both directions,
f64, the slab, both wake-up paths, the fallback and respawn; lane 5 injects
death by self-exit through the documented layout, never `terminate()` timing;
the allocation gate is the T6 retention shape (`maxBytesPerOp: 8`,
`maxMajorsPerKOp: 0`) in `npm run torture` (not part of `npm test`), counted
on each isolate, with 0 B/message as the expected target rather than the gate;
the MPMC oracle checks exactly-once results per job id.

### 5.2 Throughput is STATISTICAL-tier: once per release, on a quiet host

- Run through `quiet-lock.mjs run`; record load average and other busy `node`
  processes; a noisy host is PRELIMINARY, not a pass or a fail.
- **Interleaved A/B** against `postMessage`, K rounds, a stated statistic
  (median of per-round ratios), and a HOST verdict that exits 2 when
  inconclusive.
- **Compare like with like.** A spinning consumer burns a core; a
  `postMessage` consumer sleeps in the event loop. Report CPU time
  (`process.cpuUsage()`) next to messages/s, and also run the consumer in its
  real pattern (per-frame drain on a main thread).
- **Measure in a browser** too: Node's `worker_threads` numbers do not
  transfer (section 7 shows the ratio moving 2.5x between hosts).

### 5.3 Harness hygiene

- Accumulate into a **local** `| 0` integer, never a module-level `let SINK`
  float: those allocate a boxed number per write and swamp the signal
  (`BITWISE_RESEARCH.md` section 5).
- Any pseudo-random driver uses `Math.imul`, and its period and coverage are
  self-checked before the run is trusted (the lite-lru generator had period
  10,466 and reached 7 of 100 (key, op) pairs).
- Timing tails (p99.99, max) are not meaningful on shared runners; if a
  bound matters, witness it structurally (count CAS retries per operation)
  instead of timing it.

### 5.4 What to report

- Correctness lanes: violation and missing counts (must be 0; the control
  must be > 0).
- Messages/s and CPU s per million messages, per topology, with provenance
  (date, host, CPU, cache line, Node / browser version, load, run count).
- CAS retries per operation under contention (a structural number).
- Bytes per message (must be 0 on the hot path).

---

## 6. JS-specific constraints (why this is not just a port)

- No `Atomics.wait` on the browser main thread (4.2).
- No blocking `Atomics.wait` in a lite-worker worker either: it stops that
  worker's event loop (`post`, `lw:*`, pool `lwp:ctl`). Settled 2026-10-18,
  decisions/0002 M-07.
- COOP/COEP gating, with a transfer fallback (4.8). GitHub Pages can only get
  isolation through a service-worker header shim
  (`LiteWorkerPool/research/worker-set.md` E10).
- No SIMD, no prefetch, no control of cache-line alignment beyond index
  arithmetic inside one `SharedArrayBuffer`.
- A `SharedArrayBuffer` does not grow in all engines: capacity is fixed at
  construction, which fits the family's fixed-capacity law.
- `worker.terminate()` can stop a thread at any instruction, which turns
  Vyukov's "not formally lock-free" property into a routine operational
  hazard (4.1).

---

## 7. Measurements

### 7.1 Source study (`BITWISE_RESEARCH.md` section 2; Xeon 2.8 GHz cloud VM, Node 22.22, shared host)

| Run | Result |
|---|---|
| Vyukov CAS, 1 producer, 3 M msgs | 3.57 M msgs/s, 0 violations, 0 lost, crossed 2^31 |
| Vyukov CAS, 3 producers, 6 M msgs | 2.95 M msgs/s, 0 violations, 0 lost, crossed 2^31 |
| Racy control | 158 of 1.5 M delivered, then stalled; 8 violations |
| `postMessage(smallInt)` | 391 k msgs/s |
| `postMessage(1 KB, [transfer])` | 105-108 k msgs/s |

### 7.2 Re-probe, 2026-10-18 (PRELIMINARY)

Provenance: Apple M4 Pro, 128-byte cache lines, Node v26.8.2, load average
2.58 / 2.83 / 2.91, a WebStorm language-service `node` process running, under
`quiet-lock.mjs`, one run each. The scripts are the unmodified prototypes,
plus Appendix A.

| Run | Result |
|---|---|
| Vyukov CAS, 3 producers, 6 M msgs | 6.54 M msgs/s, 0 order/dup violations, 0 missing, crossed 2^31 |
| Racy control, 3 producers x 500 k | 228 delivered, 38 violations, 1,499,772 missing |
| `postMessage(smallInt)` | 2,070 k msgs/s |
| Producer killed between claim and publish (Appendix A) | 0 received, tail stuck at 1024 (ring full): **wedged** |

**Reading.** The algorithm's correctness reproduces exactly. The speed-up over
`postMessage` moved from 7.5-9x (source host) to about 3.2x (this host):
`postMessage` itself was 5x faster here. So "N times faster than
postMessage" is a REPORTED, host-dependent number and must be worded as a
measurement. The claim that holds across hosts is zero allocation per message.

---

## 8. Open questions

1. Is there a consumer for true MPMC (pull-mode streaming), or do SPSC lanes
   plus the ticket counter cover lite-worker and lite-worker-pool entirely?
   If the latter, Vyukov's queue stays the documented reference design and
   the shipped code is SPSC.
2. `Atomics.waitAsync` for main-thread wake-up: which engines, and is a
   feature-detected path worth a second code path?
3. Payload: fixed W words only, or a slab for variable sizes?
4. Recovery granularity for MPMC on participant death (4.1).

Answered 2026-10-18 (see `decisions/0002-mailbox.md`): 1. yes, a NEW opt-in
pool pull-mode set consumes Vyukov MPMC (M-02, M-10); SPSC lanes cover both
directions of `createWorkerSet`. 2. feature-detected on both sides; main
falls back to a per-frame drain and a worker to a caller-driven drain; a
worker never blocks in `Atomics.wait` (M-07); engine coverage becomes a
REPORTED fact in M2 / M4. 3. both: Int32 or Float64 records and a bitmap slot
slab (M-03, M-06). 4. retire the whole generation (queue + slab), re-issue unsettled jobs,
dedupe results by job id (M-10); the owner-table granularity (per-claim
column vs per-generation) is decided at M5.

---

## 9. References

- Dmitry Vyukov, "Bounded MPMC queue", 1024cores.net, lock-free algorithms /
  queues: `https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue`
  (code under Simplified BSD, (c) 2010-2011 Dmitry Vyukov).
- libcds, `cds::container::VyukovMPMCCycleQueue` documentation (records the
  "not lock-free in the official meaning" characterisation and the license):
  `https://libcds.sourceforge.net/doc/cds-api/classcds_1_1container_1_1_vyukov_m_p_m_c_cycle_queue.html`
- Boost.Lockfree `bounded_ticket_queue.hpp`, built on the same per-slot
  sequence idea; documents that a suspended producer that has claimed a slot
  blocks other producers once the ring wraps:
  `https://www.boost.org/doc/libs/develop/boost/lockfree/bounded_ticket_queue.hpp`
- spdlog's modified copy of Vyukov's queue, which reproduces his license
  notice.
- L. Lamport, "Specifying Concurrent Program Modules", ACM TOPLAS, 1983 (the
  wait-free SPSC ring).
- M. Thompson, D. Farley, M. Barker, P. Gee, A. Stewart, "Disruptor", LMAX,
  2011.
- P. Adenot, `ringbuf.js` (SPSC SharedArrayBuffer ring for the web).
- ECMAScript specification, "Memory Model" (sequentially consistent
  `Atomics`, happens-before through synchronizes-with).

---

## Appendix A: termination probe (`wedge.mjs`)

Same algorithm as `../Research/bits-prototypes/sab-queue.mjs`. Producer 2
exits immediately after its claiming CAS. Run: `node wedge.mjs`. Expected on a
correct Vyukov queue: wedged (this is the property under test, not a bug in
the port).

```js
import { Worker, isMainThread, workerData } from 'node:worker_threads';
const CAP = 1024, MASK = CAP - 1, HDR = 32;
const q = (sab) => ({ h: new Int32Array(sab, 0, HDR), seq: new Int32Array(sab, HDR * 4, CAP), dat: new Int32Array(sab, (HDR + CAP) * 4, CAP * 2) });
function enqueue({ h, seq, dat }, a, b, dieAfterClaim) {
  let pos = Atomics.load(h, 16);
  for (;;) {
    const cell = pos & MASK, s = Atomics.load(seq, cell), dif = (s - pos) | 0;
    if (dif === 0) {
      if (Atomics.compareExchange(h, 16, pos, (pos + 1) | 0) !== pos) { pos = Atomics.load(h, 16); continue; }
      if (dieAfterClaim) process.exit(0);              // terminated mid-publish
      dat[2 * cell] = a; dat[2 * cell + 1] = b; Atomics.store(seq, cell, (pos + 1) | 0); return true;
    } else if (dif < 0) return false; else pos = Atomics.load(h, 16);
  }
}
function dequeue({ h, seq, dat }, out) {
  const pos = h[0], cell = pos & MASK, s = Atomics.load(seq, cell);
  if (((s - ((pos + 1) | 0)) | 0) !== 0) return false;
  out[0] = dat[2 * cell]; out[1] = dat[2 * cell + 1];
  Atomics.store(seq, cell, (pos + CAP) | 0); Atomics.store(h, 0, (pos + 1) | 0); return true;
}
if (isMainThread) {
  const sab = new SharedArrayBuffer((HDR + CAP * 3) * 4), Q = q(sab);
  for (let i = 0; i < CAP; i++) Q.seq[i] = i;
  const M = 5000;
  const healthy = new Worker(new URL(import.meta.url), { workerData: { sab, id: 1, M, die: false } });
  await new Promise((r) => setTimeout(r, 5));
  const dying = new Worker(new URL(import.meta.url), { workerData: { sab, id: 2, M: 1, die: true } });
  await new Promise((r) => dying.on('exit', r));
  const out = new Int32Array(2); let got = 0; const t0 = performance.now();
  while (performance.now() - t0 < 1500) { if (dequeue(Q, out)) got++; }
  console.log(`healthy producer sent up to ${M}; consumer received ${got}; head=${Q.h[0]} tail=${Q.h[16]}; wedged=${got < M}`);
  healthy.terminate(); process.exit(0);
} else {
  const { sab, id, M, die } = workerData, Q = q(sab);
  for (let c = 0; c < M; c++) { while (!enqueue(Q, id, c, die)) {} }
}
```
