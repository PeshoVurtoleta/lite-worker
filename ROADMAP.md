# lite-worker roadmap

Baseline: `main` is 1.4.0 (`7558bac`). Tree clean on 2026-10-18.

Positioning: one persistent off-thread loop with a clean main thread
(`defineWorker`, RPC `call` / `post` / `on`, `frameChannel`, `adoptCanvas`).
Not a worker pool (that is `@zakkster/lite-worker-pool`, which depends on this
package; this package never depends on the pool).

Research behind track M: `RESEARCH.md` (this package) and
`../Research/BITWISE_RESEARCH.md` section 2.
Decision record: `decisions/0002-mailbox.md` (2026-10-18).

## Credit

Track M is built on **Dmitry Vyukov's bounded MPMC queue** (1024cores.net,
"Bounded MPMC queue", circa 2010-2011; his C++ code is under a Simplified BSD
license, (c) 2010-2011 Dmitry Vyukov). The per-cell sequence number, the one
CAS per operation, and the bounded, fail-on-overflow array design are his.
Every artifact this track ships (source header, README, llms.txt, CHANGELOG
entry, ADR) names him and links his page; see `RESEARCH.md` section 0 and
decisions/0002 M-11. The license check at the source (E2) is PENDING and must
pass before any M3 code.
Single-producer lanes credit Leslie Lamport's SPSC ring (1983) and Paul
Adenot's `ringbuf.js`.

## Settled

Maintainer decisions, 2026-10-18; binding. The contract that follows from
them is `decisions/0002-mailbox.md` (M-01 to M-12).

- **M-D1** `mailbox()` lives in lite-worker, next to `frameChannel`.
- **M-D2** SPSC lanes (one per producer, per direction) AND Vyukov MPMC.
- **M-D2a** MPMC is consumed only by a NEW opt-in lite-worker-pool pull-mode
  set, separate from `createWorkerSet` (never-steals law stays). Answers pool
  S-D4 (pool's own session). `../Research/SUMMARY.md` section 5 still holds:
  never MPMC for N workers -> main.
- **M-D3** `Atomics.waitAsync` feature-detected on main, per-frame drain
  otherwise; both paths gated. **M-D3a** A worker never blocks in
  `Atomics.wait`; without waitAsync its drain is caller-driven (no library
  scheduler, no postMessage doorbell).
- **M-D4** First consumer: lite-worker-pool `createWorkerSet`, both
  directions (jobs main -> worker i, results worker i -> main).
- **M-D5** Fixed records AND a slab. **M-D5a** Int32 or Float64 words per
  lane, 8-byte aligned. **M-D5b** bitmap slot slab (`sab-alloc.mjs` shape);
  a record carries a slot index; TLSF (#20) stays on hold.
  **M-D5c** one slab per producer per lane (main: one per main -> worker
  lane), so a dead worker's slab retires with its lane.
- **M-D6** The transfer fallback keeps FULL: same CAP, a credit window
  returned in one ack per drain.
- **E1** `llms.txt:198` fixed now (`[Unreleased]`); no docs-only release.

## Track M -- lossless cross-thread mailbox (design settled 2026-10-18; code not approved)

**Sequencing (maintainer, 2026-10-18):** the lite-signal 1.6.0 release has the highest
priority. Track M does not start before it ships. It is wave 5 of the suite-wide order
(`../Research/SUMMARY.md` section 1), and lite-worker-pool track S waits on it.

**Note (maintainer, 2026-10-18, later):** lite-signal 1.6.0 is paused.
Planning and doc-gap work on track M (M0-M2) may proceed; CODE (M3 and later,
any `Worker.js` edit) still needs the maintainer's explicit approval.

### Problem

- `frameChannel` (`Worker.js:148-425`) is single-producer and **latest-wins**.
  That is correct for frames and wrong for streams where every item matters.
- Everything lossless goes through `postMessage`: a structured clone, a
  `MessageEvent` and an event-loop task per message, so it allocates per
  message. lite-worker-pool measured that at ~1.1 KB/job (Node 26) and
  ~1.4 KB/job (Node 22) (`LiteWorkerPool/research/worker-set.md` E10) and
  names a SharedArrayBuffer ring as the fix.
- No lossless SAB path exists today. There are no `Atomics.wait`,
  `waitAsync` or `notify` uses in `Worker.js`.

### Placement (settled: M-D1)

**Here, as `mailbox()` next to `frameChannel`.** Not in lite-o1:

- lite-o1's roster is CLOSED at twenty-one (`LiteO1/ROADMAP.md:717`), and new
  work goes to lite-sketch.
- lite-o1's identity is "every member's headline op is O(1) worst-case or O(1)
  amortized -- and the library's job is to PROVE it" (`LiteO1/RESEARCH.md`
  section 1). A CAS retry loop is neither under contention, and Vyukov's queue
  is not formally lock-free (`RESEARCH.md` section 4.1). Its proof is a
  contended concurrency torture with a racy must-fail control, not a
  single-thread timing slope.
- lite-o1 has no `Atomics` or SharedArrayBuffer code anywhere.

Not a separate package yet, because the hard parts already live here:

- the COOP/COEP negotiation and transfer fallback (`Worker.js:187-211`; shared
  mode since 1.3.0);
- the real-SAB-only upgrade check (`Worker.js:327-328`);
- the reserved `lw:fc:*` handshake that fails closed (decisions/0001, W-10);
- worker lifecycle: the producer-death recovery in `RESEARCH.md` 4.1 needs
  code that knows a worker was terminated.

A separate package would duplicate all four. Precedent for extracting the
bare ring later: `LiteRingBuffer/ROADMAP.md:101-109` already rules that a
SAB ring is "a separate package (`lite-spsc-ring`), not a mode of this one".
Extract it when a second, non-worker consumer appears (for example an
AudioWorklet).

### Design (settled: M-D2 to M-D6; contract in decisions/0002)

From `RESEARCH.md` section 3:

- **Worker i -> main** (results, events, samples): **one SPSC lane per
  producer**, drained by main. No CAS, wait-free; a dead worker only kills its
  own lane.
- **Main -> worker i** (pool targeted jobs, commands): **one SPSC lane per
  worker**. Work never moves, so `createWorkerSet` keeps its never-steals law.
- **Vyukov MPMC** only for the N -> N pull-mode end (streaming jobs -> any
  free worker) of a NEW opt-in pool set; never N workers -> main
  (`../Research/SUMMARY.md` section 5). Recovery (decisions/0002 M-10): retire
  the generation (queue + slab), re-issue unsettled jobs, dedupe results by
  job id: exactly-once results, at-least-once execution.
- **Fixed records** of W words, **Int32 or Float64 per lane** (8-byte
  aligned). FULL is a returned result, never a silent drop or an unbounded
  buffer.
- **Variable sizes:** a bitmap slot slab (`sab-alloc.mjs` shape), one per
  producer per lane (M-D5c); a record carries a slot index; the producer alone
  allocates (CAS), consumers free (`Atomics.and`). TLSF stays on hold.
- **Transfer fallback stays lossless and bounded**, negotiated like
  `frameChannel` (`Worker.js:187-211`, `:327-328`) behind a fail-closed
  `lw:mb:*` handshake (decisions/0001, W-10); FULL is kept by a credit window
  (same CAP, one ack per drain; M-D6).
- **Wake-up:** main drains per frame, or on `Atomics.waitAsync` where
  feature-detected; both paths gated. A worker never blocks in `Atomics.wait`
  (it would stall `post` / `lw:*` / `lwp:ctl`); it uses waitAsync where
  present, otherwise the caller drives drain() (M-D3a). Size the ring for
  `peak rate / drain rate` (`RESEARCH.md` 4.2: CAP 1024 at 60 Hz caps a
  per-frame drain at ~61 k msgs/s, nominal arithmetic).
- **Lifecycle:** terminate, crash or respawn retires the dead side's lanes and
  slab; the next generation gets fresh ones. Memory a still-running thread may
  see is never reset in place.
- **Views are live only inside the drain callback** (decisions/0001, W-06);
  slab slots free when it returns.
- Hot words written by different threads sit **128 bytes apart**
  (`RESEARCH.md` 4.3).

### Gates (planned)

DETERMINISTIC tier, gated on every run by two separate scripts: `npm test`
(`test/Worker.test.js`) and `npm run torture` (`node --expose-gc
test/torture.mjs`, not part of `npm test`); full list in decisions/0002 M-12:

- Oracle: per-producer exactly-once, in-order `(producerId, counter)` on every
  SPSC lane, both directions, Int32 and Float64. MPMC: exactly-once RESULTS
  per job id at the caller (re-issue is at-least-once execution), not raw
  queue delivery.
- Lanes: uncontended; fan-in / fan-out; contended (MPMC tiny ring, slab with
  every slot in one bitmap word); wrap start at `2^31 - small`; slow consumer
  (FULL and slab exhaustion returned); death between write and publish (MPMC:
  between claim and publish, and consumer death), injected by the test worker
  driving the documented layout and exiting itself, never by `terminate()`
  timing; respawn (fresh generation, push to a retired lane returns a
  result); both wake-up paths and the transfer fallback under the same oracle.
- Controls that must fail: two producers on one SPSC lane; publish before
  write; a `<` position compare; a producer that ignores FULL; load + store
  instead of CAS on the MPMC claim and the slab bitmap (contended lanes); MPMC
  recovery without job-id dedupe; a spoofed `lw:mb:*` handshake. A real-thread
  control gates here only after M2 saw it fire in N of N runs.
- Allocation: the lite-gc-profiler retention gate (T6 shape:
  `maxBytesPerOp: 8`, `maxMajorsPerKOp: 0`) on the SAB steady state, counted on
  the main isolate and on each worker isolate (in-worker probe); a probe that
  cannot run FAILS. 0 B/message is the target (expected), not the gate.
- Parity with 1.4.0 for every existing export (`frameChannel`, `post`,
  `call`) against `git show HEAD:Worker.js`.

STATISTICAL tier, once per release, under `quiet-lock.mjs`:

- Interleaved A/B against `postMessage`, K rounds, a HOST verdict.
- CPU time reported next to messages/s.
- A browser run as well as Node.

REPORTED tier:

- Messages/s tables with provenance. Never "N x faster than postMessage" as
  a law: the ratio moved from 7.5-9x to ~3.2x between two hosts
  (`RESEARCH.md` 7).
- `Atomics.waitAsync` engine coverage and bytes per arm (engine-owned), with
  provenance.

### Order

1. **M0 (done 2026-10-18):** consumer confirmed (M-D4): lite-worker-pool
   `createWorkerSet`, both directions.
2. **M1:** ADR `decisions/0002-mailbox.md` (M-01 to M-12).
3. **M2:** torture design (decisions/0002 M-12) written **before** code, every
   control shown failing on a deliberately broken build, and each real-thread
   control's firing rate measured over N runs before it may gate in the
   DETERMINISTIC tier.
4. **M3** (needs the maintainer's explicit approval; the E2 license check
   first): SPSC lanes (Int32 / Float64), the bitmap slab, `mailbox()` roles in
   both directions, both wake-up paths, lifecycle retire, transfer fallback.
5. **M4:** the STATISTICAL gate and a browser measurement. README, llms.txt
   and CHANGELOG (each crediting Vyukov), and a new "What this is not" line.
6. **M5:** the Vyukov MPMC path with its recovery (decisions/0002 M-10) for
   the opt-in lite-worker-pool pull-mode set (M-D2a; the pool builds that set
   in its own session).

## Doc errata found during the research

- FIXED 2026-10-18 (CHANGELOG `[Unreleased]`; no docs-only release):
  `llms.txt:198` listed `WorkerHandle` as a runtime export. It is declared only
  in `Worker.d.ts:97`, not exported at runtime; the runtime exports are `VERSION` (`Worker.js:16`),
  `defineWorker` (`:807`), `frameChannel` (`:831`) and the default (`:835`).
- FIXED 2026-10-18: `RESEARCH.md` section 2 pointed to "5.3" for the 128-byte
  rule; it is 4.3.
- FIXED 2026-10-18: line refs in `ROADMAP.md` / `RESEARCH.md`. `frameChannel`
  is `Worker.js:148-425` (was cited as `:148-437`; `:427-437` is the
  canvas-control header), the full mode negotiation is `:187-211` (was
  `:187-205`, which stops inside the COI throw), and the real-SAB-only upgrade
  check is `:327-328` (was `:318-327`, which starts at a comment).
- OPEN (needs a `Worker.d.ts` edit, so not M1): `Worker.d.ts:97` is
  `export declare class WorkerHandle`, a value export to TypeScript; a value
  `import { WorkerHandle }` used with `instanceof` passes `tsc --strict` but
  fails at ESM link time, since `Worker.js` exports no `WorkerHandle`.
- OPEN, cross-package follow-up (research file, not shipped):
  `../Research/BITWISE_RESEARCH.md` cites `Worker.js:187-255` as the
  frameChannel seqlock. `frameChannel` is `Worker.js:148-425`; `:187-211` is
  the mode negotiation.
