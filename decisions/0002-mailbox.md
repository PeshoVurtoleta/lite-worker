# 0002 -- mailbox(): lossless cross-thread lanes over SharedArrayBuffer

- Status: accepted (M-D1 to M-D6 settled by the maintainer 2026-10-18; no open
  items in this record)
- Date: 2026-10-18
- Session: M1 (@zakkster/lite-worker 1.4.0, track M)
- Findings: M-D1 to M-D6, E1, E2, and the M1 planning gaps G1-G8:
  G1 the main -> worker direction was missing (M-02); G2 a blocking
  `Atomics.wait` stalls a worker's event loop (M-07); G3 a CAS-free SPSC lane
  needs its own must-fail controls (M-12); G4 real-thread controls fire
  probabilistically (M-12); G5 death is injected by self-exit, never
  `terminate()` timing (M-12 L5); G6 worker heaps are separate isolates
  (M-12 L9); G7 MPMC re-issue is at-least-once execution (M-10); G8 the slab
  bitmap has two writers (M-06)
- Scope: design contract only. CODE (M3 and later, any `Worker.js` edit) is
  NOT approved by this record; it needs the maintainer's explicit approval,
  and E2 (M-11) must pass first.

## Context

`frameChannel` (`Worker.js:148-425`) is single-producer and latest-wins: a
renderer gets the newest frame and older ones are dropped by design. Every
lossless path in the package today is `postMessage`. The pool measured that
transport at ~1.1 KB/job on Node 26 and ~1.4 KB/job on Node 22
(LiteWorkerPool `research/worker-set.md` E10, disclosed at
`WorkerPool.js:397-398`). `Worker.js` contains no `Atomics.wait`,
`Atomics.notify` or `Atomics.waitAsync` anywhere.

Track M adds a lossless, bounded, allocation-free mailbox. The research is
`RESEARCH.md`; the maintainer settled the design on 2026-10-18:

- **M-D1** placement in lite-worker (M-01).
- **M-D2** SPSC lanes AND Vyukov MPMC; **M-D2a** MPMC only for a NEW opt-in
  pool pull-mode set (M-02).
- **M-D3** waitAsync feature-detected on main, per-frame drain otherwise;
  **M-D3a** caller-driven drain in a worker without waitAsync (M-07).
- **M-D4** first consumer: lite-worker-pool `createWorkerSet`, both
  directions (jobs main -> worker i, results worker i -> main).
- **M-D5** fixed records AND a slab; **M-D5a** Int32 or Float64 words per
  lane (M-03); **M-D5b** bitmap slot slab; **M-D5c** one slab per producer
  per lane (M-06).
- **M-D6** the transfer fallback keeps FULL via a credit window (M-08).
- **E1** `llms.txt:198` listed `WorkerHandle` as a runtime export; it is
  declared only in `Worker.d.ts:97`, not exported at runtime. Fixed now under CHANGELOG `[Unreleased]`; no
  docs-only release.
- **E2** the license check at the source is PENDING (M-11).

This record pins the contract that follows. Every claim in it is tiered
(M-12); track M owns no measured number yet, and the numbers quoted below
carry their provenance.

## M-01 -- Placement (M-D1)

`mailbox()` lives in `Worker.js`, next to `frameChannel`. It reuses the mode
negotiation (`Worker.js:187-211`), the typed-plane handshake and the handle
lifecycle. Extraction to a separate `lite-spsc-ring` package waits for a
second, non-worker consumer (`../LiteRingBuffer/ROADMAP.md:101-109` already
names that package as the home of a SAB SPSC ring).

## M-02 -- Topology (M-D2, M-D2a, M-D4)

| Direction | Structure | Claimants (prod / cons) | Consumer |
|---|---|---|---|
| worker i -> main | SPSC lane per worker | 1 / 1 | `createWorkerSet` results, events |
| main -> worker i | SPSC lane per worker | 1 / 1 | `createWorkerSet` targeted jobs; never steals |
| main -> any free worker (N -> N pull mode) | Vyukov MPMC, one per generation | 1..N / N | NEW opt-in pool pull-mode set only (M-D2a) |

- MPMC is never the N workers -> main transport (`../Research/SUMMARY.md`
  section 5: a producer killed mid-publish wedges the queue). Pull-mode
  results come back on the per-worker SPSC lanes.
- `createWorkerSet` keeps its never-steals law (`WorkerPool.js:390-392`):
  work never moves between per-worker lanes.
- MPMC ships ONE body: Vyukov's exact protocol, CAS on both ends. A single
  producer pays one uncontended CAS; that keeps one copy of the hot bytes and
  exact credit to the published algorithm. No SPMC / MPSC special case.

## M-03 -- SPSC lane layout + typing (M-D5a)

- **Ring.** Lamport ring, positions only, no `seq` array. Empty is
  `tail === head`; FULL is `((tail - head) | 0) === CAP`. Every position
  compare is `(a - b) | 0` against 0, never `a < b` (wrap at 2^31). CAP is a
  power of two below 2^31.
- **Header (Int32 words).** Word 0: HEAD, with the consumer's ARMED flag in
  the same line (consumer-only writes; M3 picks the word). Word 32: TAIL
  (producer-only line). Word 64 and up: a cold line with GEN, STATE, CAP, W,
  TYPE (0 = i32, 1 = f64), written at construction and retirement only.
- **Payload** starts at byte 384 (128- and 8-aligned). Lanes that share one
  SAB use a stride rounded up to 128 B.
- **Why 128 B.** Hot words 128 B apart sit in different 128 B lines and in
  different Intel 64 B adjacent-line prefetch pairs, whatever the SAB base
  alignment (`RESEARCH.md` 4.3, 6).
- **Records** are W words of the lane's type. A Float64Array view needs a
  byte offset that is a multiple of 8; the layout guarantees it.
- **i32 lanes** store ToInt32(v), exactly as an Int32Array write does:
  fractions truncate toward zero, out-of-range values wrap mod 2^32, and
  NaN / +-Infinity store 0, all silently. The hot path does not validate; a
  NaN stored as 0 is a fail-open the caller owns (validate before `push`, or
  use an f64 lane).
- **f64 lanes** carry numbers, not bit patterns: NumericToRawBytes may
  canonicalise a NaN payload; -0 is preserved.
- **Documented contract.** The layout is published like the `frameChannel`
  LAYOUT comment (`Worker.js:213-217`). Lane L5 (M-12) drives it directly, so
  changing it is a breaking change.
- **Zero-box.** HEAD and TAIL stay in Int32Array slots, never cached in JS
  fields (`RESEARCH.md` 4.6). Callbacks receive `(array, offset)`, never a
  computed double.

## M-04 -- FULL is a returned result

`push` reports four distinct outcomes besides success: lane FULL, slab
exhausted, slot too small, lane RETIRED. None throws on the hot path, none is
a silent drop or an unbounded buffer, none is `null`. A producer that sees
RETIRED stops (fails closed). M3 picks the encoding.

Sizing: `CAP >= peak rate / drain rate x safety`. `RESEARCH.md` 4.2's ~61 k
msgs/s ceiling (CAP 1024 drained once per frame at 60 Hz) is nominal
arithmetic, not a measurement.

## M-05 -- Live-view contract (W-06 parity)

The drain callback gets the lane array and a record offset. That view is
valid only inside the callback; HEAD is stored after the callback returns
(read before hand-back, `RESEARCH.md` 4.5). A slab slot named by the record
is freed by the library when the callback returns. A caller that keeps data
copies it out (a `readInto` analogue).

No "is this view still valid" helper, for the same reasons as decisions/0001
W-06: it would imply a safety the ring cannot give, and it invites the
hold-and-poll pattern the ring exists to prevent.

## M-06 -- Slab (M-D5b, M-D5c, G8)

Variable-size records use a bitmap slot slab in the
`../Research/bits-prototypes/sab-alloc.mjs` shape, which is already
two-writer: alloc claims a bit with CAS (`:19`), free clears it with
`Atomics.and` (`:43`) and re-sets the summary hint with `Atomics.or`; alloc
re-checks a word after clearing its hint so a racing free is never lost
(`:33-35`).

- **The producer is the ONLY allocator; consumers free.** Rationale:
  1. the MPMC end has N freeing consumers; a back-channel SPSC "return" lane
     would need N of them;
  2. it keeps the maintainer's prototype shape;
  3. with one allocator a CAS fails only when a free cleared a bit in that
     word since the load, so a word visited costs at most 32 failed CAS
     (expected, by argument; witnessed structurally by counting retries, not
     by timing, `RESEARCH.md` 5.3).
- **Granularity (M-D5c, settled 2026-10-18).** One slab per (producer, lane).
  Main gets one slab per main -> worker lane: a dead worker's slab retires
  with its lane, and no zombie `Atomics.and` from a browser `terminate()`
  that did not stop the thread races a slab another lane still uses.
  Rejected: one shared slab per main.
- **Slots.** Slot size is fixed per slab; one slot per record; the record
  carries slot index + length. `SLOTS >= CAP + 1` makes exhaustion
  unreachable while every record uses one slot (by construction; L4 checks).
- TLSF stays on hold. The contended-lane control (C6) applies.

## M-07 -- Wake-up (M-D3, M-D3a, G2)

**Main** never calls `Atomics.wait` (it throws on the browser main thread).
If `typeof Atomics.waitAsync === "function"`, main arms one waitAsync on TAIL
with the last-seen value; otherwise the caller drives `drain()` once per
frame. waitAsync's expected-value check makes the wake race-free only in this
order: the consumer stores ARMED (`Atomics.store`) BEFORE it calls
waitAsync, and the producer loads ARMED (`Atomics.load`) AFTER its TAIL
store, never reusing the value it read with HEAD for the FULL check (that
read precedes the TAIL store and can lose the wake for a burst's last
record). With sequentially consistent Atomics, either the waiter's check
sees the new TAIL or the producer sees ARMED and notifies. ARMED only lets
the producer skip `Atomics.notify`; the re-load hits a line the producer
already reads for FULL. An option forces the per-frame path so both paths are gated.

**A worker** never blocks in `Atomics.wait`. A blocked worker stops its event
loop: `post` / `call` replies, `lw:fc:*`, `lw:canvas*`, pool `lwp:ctl`, and the
kill / respawn handshakes all stall, and a waiter whose producer died can only
be terminated. Rejected:

- a bounded-timeout wait: it blocks control traffic up to the timeout on
  every idle period, and the timeout is a guess;
- "wait only when the worker has no typed-plane traffic": every
  `defineWorker` worker has a typed plane, and the first consumer uses
  `lwp:ctl`.

So a worker uses waitAsync where present; otherwise its drain is
CALLER-DRIVEN (**M-D3a**, settled 2026-10-18): the library ships no
scheduler, and the worker's own loop (e.g. the pool's) calls `drain()`, the
way a renderer drives `read()` from its rAF. Rejected for M-D3a: a library
`setTimeout` poll; a postMessage doorbell (allocates per edge, breaks the
0 B target).

**Allocation disclosure.** waitAsync allocates its result object and a
Promise per arm (engine memory), disclosed the way the pool discloses
MessagePort (`WorkerPool.js:397-398`). Under load that is one arm per
empty -> non-empty edge, not one per message.

## M-08 -- Fallback (M-D6)

- **Modes** `"auto"` / `"shared"` / `"transfer"`, as `frameChannel`.
  Negotiation reuses the COI gate (`Worker.js:187-207`) and the post()/on()
  requirement (`:208-211`).
- **Upgrade.** The consumer starts on the fallback and upgrades only on a
  real `SharedArrayBuffer` whose byteLength, CAP, W, TYPE and GEN all match:
  the same fail-closed guard as `lw:fc:sab` (`Worker.js:327-328`,
  decisions/0001 W-10).
- **Namespace.** `lw:mb:*` is reserved and documented exactly like `lw:fc:*`
  and `lw:canvas*`: callers must not post those types.
- **Same delivery contract** on both paths: lossless, FIFO per producer, the
  M-05 live-view rule.
- **FULL is kept (M-D6, settled 2026-10-18):** same CAP, enforced by a credit
  window the consumer returns in one ack per drain. Rejected: an unbounded
  postMessage fallback (code tested without COOP/COEP would never see FULL).
- **Allocation.** The fallback allocates per message (E10's transport cost).
  The 0 B target covers the SAB path only.

## M-09 -- Lifecycle

Retire, never reset in place: a browser `terminate()` does not confirm that
the thread stopped, so memory a still-running thread may touch is never
reused.

- A dead or terminated side's lanes and slab are retired. The next generation
  gets a fresh SAB on the cold path, like the pool's fresh per-start buffers
  (`WorkerPool.js:600-603` in `_start`, `:595-631`).
- Late old-generation writes land in memory nobody reads; old-gen-tagged
  messages are ignored (the pool's gen guards, `WorkerPool.js:634`, `:649`).
- Before retiring, the consumer drains every published record. Publish (the
  TAIL store) is the commit point; an unpublished partial write is invisible.
- Hooks: M3 adds a cold hook to `terminate()` / `destroy()`
  (`Worker.js:721-736`, `:738-744`; today neither knows about channels). In
  the pool, retirement goes in `_down` (`WorkerPool.js:653-681`, gen bump
  `:657`) and new lanes in `_start` via `respawn` (`:994-1012`). That part is
  cross-package and lands in the pool's own session.
- This refines `RESEARCH.md` 4.1's "reset by its consumer": retired and
  replaced, not reset.

## M-10 -- MPMC recovery (M5, G7)

A generation is a queue SAB plus a slab SAB, retired together, never reused.
On any participant's death main:

1. seals the old generation;
2. takes back unclaimed records by claiming them itself (main is a legal
   claimant);
3. drains the published results;
4. re-issues every unsettled job it cannot prove a live worker holds;
5. moves the survivors to the new generation with an `lw:mb:gen` handshake
   (W-10 guard).

Execution is AT-LEAST-once; results reach the caller EXACTLY once. Main
settles each job id once; a later result for a settled or unknown id is
dropped and counted (`dupes`), never delivered. The M2 oracle checks
caller-visible settlements and REPORTS re-executions; raw queue counts are
not the oracle. Owner granularity (a per-claim owner column vs coarse
per-generation) is decided at M5, measured by the re-execution count.

## M-11 -- Credit + license (E2)

- **Algorithm.** The MPMC is Dmitry Vyukov's bounded MPMC queue (per-cell
  sequence number, one CAS per operation, bounded and fail-on-overflow):
  https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
- **Prior art.** The SPSC lanes are Leslie Lamport's single-producer /
  single-consumer ring (1983). Paul Adenot's `ringbuf.js` is the reference
  SAB SPSC ring for the web.
- **License.** Simplified BSD, (c) 2010-2011 Dmitry Vyukov, as reproduced in
  the libcds and spdlog mirrors (`RESEARCH.md` sections 0 and 9).
- **E2 PENDING.** The source page was tried twice on 2026-10-18: once it
  returned a redirect to a loopback address from this host; once the
  built-in browser navigation failed. The license MUST be confirmed at the
  source before any M3 code.
- **Notice.** If shipped code is a line-by-line translation of his listing,
  his notice goes in the source header and a README NOTICE section. Either
  way the source header, README, llms.txt and the CHANGELOG entry that
  introduce the structure name him and link his page.

## M-12 -- M2 torture design

Written in M2, before any code. Tier is DETERMINISTIC (gated in `npm test` /
`npm run torture`, `node --expose-gc test/torture.mjs`) unless a line says
otherwise.

Oracle: per-producer exactly-once, in-order `(producerId, counter)` on every
SPSC lane, both directions, Int32 and Float64. MPMC: exactly-once RESULTS per
job id at the caller (M-10), not raw queue delivery.

Lanes:

- **L1** uncontended, both directions, i32 + f64 (f64 round trip exact
  except NaN payload bits). DETERMINISTIC.
- **L2** fan-in N -> main and fan-out main -> N. DETERMINISTIC.
- **L3** wrap: positions start at `2^31 - small`. DETERMINISTIC.
- **L4** slow consumer: FULL and slab results are returned;
  accepted + FULL = attempted; exhaustion unreachable at `SLOTS >= CAP + 1`.
  DETERMINISTIC.
- **L5** (G5) death between write and publish: the test worker writes a
  payload through the documented layout (M-03) and exits itself
  (`process.exit` / `self.close()`) without storing TAIL. MPMC: CAS TAIL then
  exit; and a consumer that CASes HEAD then exits. Never `terminate()`
  timing; shipped code carries no test hooks. DETERMINISTIC.
- **L6** contended: an MPMC tiny ring (N x N) and a 32-slot slab (every slot
  in one bitmap word, `RESEARCH.md` 4.4). DETERMINISTIC.
- **L7** respawn: fresh generation, old-gen writes ignored, push after retire
  returns RETIRED, 0 silent drops. DETERMINISTIC.
- **L8** transfer fallback under the same oracle, including FULL via the
  credit window, plus the spoof control (C8). DETERMINISTIC.
- **L9** (G6) allocation: the T6 retention gate (`maxBytesPerOp: 8`,
  `maxMajorsPerKOp: 0`, stabilize; `test/torture.mjs:568`) on EACH isolate:
  main, and every worker through an in-worker probe under the same
  `--expose-gc` process. A probe that cannot run is a FAIL, never 0.
  "0 B/message" stays the design target, labelled expected. DETERMINISTIC.
- **L10** wake-up: the per-frame and waitAsync paths each run the oracle
  (DETERMINISTIC); waitAsync counts R arms over M messages; bytes per arm are
  REPORTED; the gate is "allocations follow arms, not messages"
  (DETERMINISTIC).
- **Parity:** `frameChannel` / `post` / `call` against
  `git show HEAD:Worker.js`. DETERMINISTIC.

Controls (each must make its gate fail):

- **C1** two producers on one SPSC lane (real threads, probabilistic). Tier
  pending M2 firing rate.
- **C2** publish before write (real threads, probabilistic). Tier
  pending M2 firing rate.
- **C3** a `<` position compare in the wrap lane (single thread).
  DETERMINISTIC.
- **C4** a producer that ignores FULL (single thread). DETERMINISTIC.
- **C5** load + store instead of CAS on the MPMC claim, in L6 (real threads,
  probabilistic). Tier pending M2 firing rate. Precedent, not a firing rate:
  `RESEARCH.md` 7.2's racy control (3 producers x 500 k) delivered 228,
  with 38 violations and 1,499,772 missing (Apple M4 Pro, Node v26.8.2,
  load ~2.6-2.9, one run, PRELIMINARY).
- **C6** plain read-modify-write instead of CAS on the slab bitmap, in L6
  (real threads, probabilistic; `sab-alloc.mjs` already has a `racy` mode).
  Tier pending M2 firing rate.
- **C7** MPMC recovery without job-id dedupe: an injected duplicate result
  must double-settle. DETERMINISTIC.
- **C8** a spoofed `lw:mb:sab` handshake (plain ArrayBuffer, matching
  layout) must leave the consumer on the fallback. DETERMINISTIC.
- **C9** a drain that retains an allocation per message must fail L9.
  DETERMINISTIC.

Rule for C1 / C2 / C5 / C6 (G4): M2 records each control's firing rate over N
runs (N chosen and recorded in M2; nominal 100). A control gates in the
DETERMINISTIC tier only after it fired in N of N runs on the reference host,
with provenance (precedent: T3's `torn` control, `test/torture.mjs:394`).
Otherwise M2 either redesigns it (a scripted single-thread interleaving
against the mutant) or moves it to the STATISTICAL tier (once per release, K
rounds, a HOST verdict that exits 2).

## Consequences

- No change to any existing export, type or hot body until M3; `Worker.js`
  and `Worker.d.ts` are untouched by M1.
- A new reserved typed-message namespace, `lw:mb:*` (`lw:mb:sab`,
  `lw:mb:gen`), documented like `lw:fc:*`.
- Every claim is tiered: correctness, parity and allocation counts are
  DETERMINISTIC; throughput is STATISTICAL (M4, once per release, quiet host);
  benchmark tables and waitAsync bytes per arm are REPORTED with provenance.
  No "N times faster than postMessage" claim: `RESEARCH.md` 7.2 shows the
  ratio is host-dependent.
- Track M owns no measured number yet; the `RESEARCH.md` section 7 numbers
  quoted here carry their provenance.
- Cross-package work (pool `_start` / `_down` / `respawn` hooks, the opt-in
  pull-mode set) lands in lite-worker-pool's own sessions.
- Order: M2 (torture design, controls proven failing) -> M3 (SPSC lanes, slab,
  both wake-up paths, retire, fallback; needs approval and E2) -> M4
  (STATISTICAL gate, browser measurement, docs crediting Vyukov) -> M5 (MPMC
  and its recovery).
