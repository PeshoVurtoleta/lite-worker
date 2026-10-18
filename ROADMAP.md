# lite-worker roadmap

Baseline: `main` is 1.4.0 (`7558bac`). Tree clean on 2026-10-18.

Positioning: one persistent off-thread loop with a clean main thread
(`defineWorker`, RPC `call` / `post` / `on`, `frameChannel`, `adoptCanvas`).
Not a worker pool (that is `@zakkster/lite-worker-pool`, which depends on this
package; this package never depends on the pool).

Research behind track M: `RESEARCH.md` (this package) and
`../Research/BITWISE_RESEARCH.md` section 2.

## Credit

Track M is built on **Dmitry Vyukov's bounded MPMC queue** (1024cores.net,
"Bounded MPMC queue", circa 2010-2011; his C++ code is under a Simplified BSD
license, (c) 2010-2011 Dmitry Vyukov). The per-cell sequence number, the one
CAS per operation, and the bounded, fail-on-overflow array design are his.
Every artifact this track ships (source header, README, llms.txt, CHANGELOG
entry, ADR) names him and links his page; see `RESEARCH.md` section 0 for the
exact attribution and the license check required before code ships.
Single-producer lanes credit Leslie Lamport's SPSC ring (1983) and Paul
Adenot's `ringbuf.js`.

## Settled

Nothing in track M is settled yet. Decisions M-D1 to M-D5 below wait on the
maintainer.

## Track M -- lossless cross-thread mailbox (proposal, not scheduled)

**Sequencing (maintainer, 2026-10-18):** the lite-signal 1.6.0 release has the highest
priority. Track M does not start before it ships. It is wave 5 of the suite-wide order
(`../Research/SUMMARY.md` section 1), and lite-worker-pool track S waits on it.

### Problem

- `frameChannel` (`Worker.js:148-437`) is single-producer and **latest-wins**.
  That is correct for frames and wrong for streams where every item matters.
- Everything lossless goes through `postMessage`: a structured clone, a
  `MessageEvent` and an event-loop task per message, so it allocates per
  message. lite-worker-pool measured that at ~1.1 KB/job (Node 26) and
  ~1.4 KB/job (Node 22) (`LiteWorkerPool/research/worker-set.md` E10) and
  names a SharedArrayBuffer ring as the fix.
- No lossless SAB path exists today. There are no `Atomics.wait`,
  `waitAsync` or `notify` uses in `Worker.js`.

### Placement (recommendation; see M-D1)

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

- the COOP/COEP negotiation and transfer fallback (`Worker.js:187-205`; shared
  mode since 1.3.0);
- the real-SAB-only upgrade check (`Worker.js:318-327`);
- the reserved `lw:fc:*` handshake that fails closed (decisions/0001, W-10);
- worker lifecycle: the producer-death recovery in `RESEARCH.md` 4.1 needs
  code that knows a worker was terminated.

A separate package would duplicate all four. Precedent for extracting the
bare ring later: `LiteRingBuffer/ROADMAP.md:101-109` already rules that a
SAB ring is "a separate package (`lite-spsc-ring`), not a mode of this one".
Extract it when a second, non-worker consumer appears (for example an
AudioWorklet).

### Design (recommendation; see M-D2)

From `RESEARCH.md` section 3:

- **N workers -> main** (results, events, profiler samples): **one SPSC lane
  per producer**, drained by the main side. No CAS, no contention, wait-free,
  and a terminated worker only kills its own lane. The handle resets that
  lane on terminate or respawn.
- **Vyukov's MPMC protocol** is the reference design, kept for a real
  multi-claimant end (pull-mode streaming to N workers). It ships only with
  the recovery story in `RESEARCH.md` 4.1: retire and rebuild the queue on any
  participant's death, then re-issue from an owner table.
- **Fixed-width int32 records** (W words per message). FULL is a returned
  result, never a silent drop or an unbounded buffer.
- **Transfer fallback stays lossless**: without isolation, `mailbox()` rides
  `postMessage` with the same delivery contract.
- **Main-thread drain** once per frame (and `Atomics.waitAsync` where
  feature-detected; M-D3). Size the ring for `peak rate / drain rate`
  (`RESEARCH.md` 4.2: CAP 1024 at 60 Hz caps at ~61 k msgs/s).
- **Payload views are live only inside the drain callback**: the same
  contract as `frameChannel` (decisions/0001, W-06).
- `head` / `tail` / lane indices sit **128 bytes apart** (Apple Silicon lines
  are 128 B; `RESEARCH.md` 4.3).

### Gates (planned)

DETERMINISTIC tier, in `npm test`:

- A per-producer exactly-once, in-order oracle.
- Lanes:
  1. uncontended;
  2. contended (tiny ring);
  3. wrap start at `2^31 - small`;
  4. slow consumer (FULL reported);
  5. producer terminated between write and publish (and, for any MPMC path,
     between claim and publish, and consumer death);
  6. the racy control, which **must fail in the contended lane**.
- 0 B/message on the steady-state path under
  `node --expose-gc test/torture.mjs`.
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

### Order

1. **M0:** confirm the consumer (M-D4). lite-worker-pool's result path is the
   candidate.
2. **M1:** ADR `decisions/0002-mailbox.md`: topology, Vyukov credit and
   license, recovery, fallback contract.
3. **M2:** torture design (the lanes above) written **before** code, with the
   racy and termination controls shown failing on a deliberately broken
   build.
4. **M3:** implement the SPSC lanes, the `mailbox()` producer and consumer
   roles, and the transfer fallback.
5. **M4:** the STATISTICAL gate and a browser measurement. README, llms.txt
   and CHANGELOG (each crediting Vyukov), and a new "What this is not" line.
6. **M5** (only if M-D2 picks it): an MPMC path with recovery.

## Decisions waiting on the maintainer

- **M-D1** Placement: `mailbox()` in lite-worker (recommended), a sibling
  package now, or `lite-spsc-ring` first.
- **M-D2** Shipped topology: per-producer SPSC lanes only (recommended),
  or Vyukov MPMC as well.
- **M-D3** `Atomics.waitAsync` wake-up path: yes (feature-detected) or
  per-frame polling only.
- **M-D4** The first consumer to build for: lite-worker-pool results,
  input / command events, or lite-hud profiler samples.
- **M-D5** Payload: fixed W int32 words only, or also a slab for variable
  sizes (`../Research/bits-prototypes/sab-alloc.mjs`).

## Doc errata found during the research (not yet fixed)

- `llms.txt:198` says Worker.js "exports ... `WorkerHandle`". The runtime
  exports are `VERSION` (`Worker.js:16`), `defineWorker` (`:807`),
  `frameChannel` (`:831`) and the default (`:835`). `WorkerHandle` is a
  type-only declaration (`Worker.d.ts:97`). Reword it as "type
  `WorkerHandle` (in Worker.d.ts)".
- `../Research/BITWISE_RESEARCH.md` cites `Worker.js:187-255` as the
  frameChannel seqlock. `frameChannel` is `Worker.js:148-437`; `:187-205` is
  the mode negotiation. That file is research, not shipped.
