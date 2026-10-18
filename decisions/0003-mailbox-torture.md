# 0003 -- mailbox() torture design

- Status: proposed
- Date: 2026-10-18
- Session: M2 (@zakkster/lite-worker 1.4.0, track M)
- Findings: expands decisions/0002 M-12 (G3, G4, G5, G6); amends M-04 and
  M-08 (decisions/0002 Amendment A4)
- Scope: design only. No edit to `Worker.js`, `Worker.d.ts` or `test/` is
  made or approved by this record. The design needs 0 exports beyond the
  decisions/0002 surface, 0 new flags and 0 test hooks in shipped code.
  CODE (M3 and later) still needs the maintainer's explicit approval; E2
  has passed (decisions/0002 A6).

## Context

decisions/0002 M-12 lists the lanes and controls of the mailbox torture in
one paragraph each. It leaves open how a probabilistic real-thread control is
gated (G4), how death is injected without a shipped hook (G5), and how each
worker isolate is measured (G6). This record fixes those, before any code, so
M3 builds against a written oracle rather than inventing one while it codes.

The suite law applies: every gate ships a control that must fail, the oracle
is exact for every interleaving, and each run does a fixed op count, never a
wall-clock amount. A measurement is a mutant build, and a mutant build is
code, so no firing rate is measured in M2: SPSC, slab and wake-up controls
are measured in M3, the MPMC control (C5) in M5. Every rate quoted below is
labelled expected, not measured.

Precedents in `test/torture.mjs` that this design reuses (line refs checked
2026-10-18):

- stamp-all-equal check over a record's words: the T3 reader, `:368`;
- serialized-producer SPLICE and the "source drifted" failure: `:702-711`;
- spoofed handshake plus its positive half: `:745-770`;
- T6 retention gate, `maxBytesPerOp: 8`, `maxMajorsPerKOp: 0`: `:568`;
- heap delta after settle over a fixed count (transfer path): `:442-448`;
- pinned-handle retention control: `:685`;
- layout conformance step: T8, `:935`;
- the `torn` firing precedent: `:394`;
- self-contained factory serialized into `_source`: `Worker.js:533`
  (`__makeFrameChannel`).

## T-00 -- M3 dependencies the tests rely on

The tests read nothing from shipped code beyond the public API, but they
depend on five documented M3 choices. If M3 does not make one, the lane that
needs it is blocked and M3 must say so.

- **D1.** The lane factory is serialized into `_source` as a self-contained
  function, like `__makeFrameChannel` (`Worker.js:533`), with stable SPLICE
  anchors so a mutant can be cut from the shipped body.
- **D2.** The ARMED word is documented (M3's pick; the review nit was header
  word 1; decided, see "M3 decisions"). C10-s and the layout conformance
  step depend on it. The 128 B inter-lane stride (a 128 B GAP between hot
  words, not just rounding) is documented; L2 (lanes sharing one SAB) and
  conformance depend on it.
- **D3.** The slab bitmap layout is documented (C6-s, `slabRestOk`).
- **D4.** `Atomics.waitAsync` is looked up at construction or later, never at
  module load, so L10's arm counter can wrap it.
- **D5.** The MPMC layout is documented at M5.

## T-01 -- Files (planned; none created in M2)

- `test/torture/mailbox.mjs`: the tier bodies and the shared predicates
  `oracleOk`, `fullAcctOk(accepted, full, attempted, delivered)`,
  `slabRestOk(popcount)`, `wakeOk(stranded)`, `handshakeOk`, `settledOnceOk`.
  The predicates are shared by the real tiers and the controls (the
  `torture.mjs:47` convention).
- `test/torture.mjs` registers them after T9 as tiers M1-M10, MP, MC: one
  entry point, one "ok", T0-T9 byte-identical. Inline bodies would double a
  1050-line file.
- `test/torture/mb-layout.mjs`: the ONLY test-side copy of the M-03 layout
  (HEAD at word 0, TAIL at word 32, cold line at word 64 and up, payload at
  byte 384, plus the slab and ARMED words). A conformance step checks it
  against a live lane (T8 precedent, `torture.mjs:935`), so a layout change
  fails one place.
- `test/torture/mb-entry.mjs`: the worker entry, modes `run`, `inject`,
  `probe`. `thread-entry.mjs` is untouched.
- `test/torture/mb-firing.mjs`: the G4 runner (T-06). NOT part of
  `npm run torture`.
- `test/torture/parity-trace.mjs` and
  `test/torture/golden/parity-1.4.0.json` (T-08).

## T-02 -- Lanes

Every lane is DETERMINISTIC: the oracle is exact for every interleaving (a
per-producer `(producerId, counter)` check, a conservation count, or a
structural predicate) and each run performs a fixed op count. Where a lane
has real threads, the thread timing changes which interleaving runs, never
the verdict.

| Lane | Files | Threads | Oracle |
|---|---|---|---|
| L1 | mailbox.mjs, mb-entry.mjs | main + 1 worker | `oracleOk` |
| L2 | mailbox.mjs, mb-entry.mjs | main + 4 workers | `oracleOk` per producer |
| L3 | mailbox.mjs, mb-layout.mjs | main + 1 worker | `oracleOk` + TAIL < 0 |
| L4 | mailbox.mjs | main only | `fullAcctOk`, 0 exhaustion |
| L5 | mailbox.mjs, mb-entry.mjs, mb-layout.mjs | main + 1 worker | K (K+1) delivered |
| L6 | mailbox.mjs, mb-entry.mjs, mb-layout.mjs | main + 1 worker (M5: 4+4) | stamp, `slabRestOk` |
| L7 | mailbox.mjs, mb-entry.mjs, mb-layout.mjs | main + 3 generations x 1 worker | `oracleOk`, 0 stale |
| L8 | mailbox.mjs, mb-entry.mjs | main + 1 worker | `oracleOk`, `handshakeOk` |
| L9 | mailbox.mjs, mb-entry.mjs | main + 1..4 workers | per-isolate T6 gate |
| L10 | mailbox.mjs | main + 1 worker (Q5: decided) | `oracleOk`, `wakeOk`, arms |
| MP | parity-trace.mjs, golden | main + 1 worker | deep-equal trace |

- **L1 uncontended.** 1 worker, 4 lanes (worker -> main and main -> worker,
  each i32 and f64), 1e5 records per lane. The f64 values include -0, +-Inf,
  2^53 and a denormal; NaN is checked with `Number.isNaN` only (payload bits
  may canonicalise, decisions/0002 M-03). The i32 lane is compared against
  `v|0`. Deterministic: single producer, single consumer, exact oracle.
- **L2 fan-in / fan-out.** 4 workers, fan-in 4 -> main and fan-out
  main -> 4: 5 isolates. One lane per producer, so the oracle is per
  producer. Deterministic: lanes are independent and each is SPSC.
- **L3 wrap.** Single thread plus 1 worker. `mb-layout` sets
  HEAD = TAIL = 2^31 - 4 while both sides idle; the lane then moves
  4 x CAP records and the test checks TAIL went negative (so the wrap was
  crossed, not merely approached). Deterministic: the start state is written
  while idle, the oracle is exact.
- **L4 slow consumer.** Single thread. CAP accepted, then FULL;
  `fullAcctOk` holds (accepted + FULL = attempted, accepted = delivered).
  With SLOTS = CAP + 1, 1e5 mixed ops return SLAB_EXHAUSTED 0 times. With
  SLOTS = CAP - 1 it must be returned at least once (proves the path
  exists). `len` greater than the slot size returns SLOT_TOO_SMALL.
  Deterministic: one thread, no interleaving.
- **L5 death between write and publish.** See T-03.
- **L6 contended.** Real threads. A 32-slot slab (one bitmap word), 1
  producer worker allocating, main freeing. Each slot is stamped with its
  record id and checked all-equal (the T3 shape, `torture.mjs:368`);
  `slabRestOk` at rest (popcount 0 once every record is drained). M5 adds the
  MPMC CAP 4 queue with 4 x 4 threads. Deterministic: stamp equality and the
  rest popcount hold for every interleaving of a correct build.
- **L7 respawn.** 3 generations of 1 worker each. After retire, the test
  writes into the tapped old SAB through `mb-layout` and stores TAIL: 0
  records are delivered. Old-generation `lw:mb:*` messages are ignored. A
  push after retire returns RETIRED. Per generation accepted === delivered.
  Retention: 4096 create / push / drain / retire cycles, then
  `tracker.size() === 0` after 2 settles, plus a pinned-lane control (T9
  shape, `torture.mjs:685`) that keeps size >= 1. Deterministic: retire is a
  state transition, not a race.
- **L8 transfer fallback.** Transfer mode forced, and auto mode without COI.
  FULL at CAP + 1 until the drain ack returns credit. Carries C8, the
  rejected-SAB cases (T-07), and the pre-verdict rule: pushes before the ack
  go over the fallback; 0 lost, 0 duplicated across the switch.
  Deterministic: the credit window is a count.
- **L9 allocation.** See T-04.
- **L10 wake-up.** The per-frame path (forced, drain on `setImmediate`) and
  the waitAsync path each run under the oracle. Single thread: R scripted
  bursts of 1000 give exactly R arms (counted by a test wrapper, D4). Heap
  delta after settle divided by M messages <= 8 B (heap-delta shape,
  `torture.mjs:442-448`). Bytes per arm are REPORTED, not gated. After each
  burst's last push, `Atomics.notify(i32, TAIL_WORD, 1) === 0` (no stranded
  waiter, `wakeOk`). Deterministic: arms follow bursts, not messages, and
  the stranded count is exact.
- **MP parity.** `frameChannel`, `post`, `call` and the export keys against
  `git show HEAD:Worker.js` (T-08). Deterministic: a recorded trace compared
  deep-equal.

## T-03 -- L5 injector (no shipped test hooks)

- The inject-mode shim (test code) taps the outgoing `lw:mb:sab` and exposes
  its SAB as `self.__mbTap`.
- The test body pushes K records through the real API, then writes record
  K + 1 through `mb-layout` at `Atomics.load(TAIL) & (CAP - 1)`: d1 writes
  W/2 words; d2 writes all W words; d4 also sets the record's slab bit and
  slot. Then `process.exit(0)` without storing TAIL.
- d3 exits right after the TAIL store: the control; K + 1 must be delivered.
- K = 0, 1, CAP - 1, plus one run on a wrapped lane.
- On 'exit', main drains then retires. Exactly K records (d3: K + 1) arrive,
  in order.
- Reverse direction: the worker consumer exits inside its drain callback,
  before the HEAD store. Main's next push returns RETIRED, never FULL
  forever.
- MPMC (M5): CAS TAIL then exit; a consumer that CASes HEAD then exits. The
  layout is D5.

Deterministic: the publish (TAIL store) is the commit point (decisions/0002
M-09), so an unpublished partial write is invisible for every interleaving.

## T-04 -- L9 in-worker probe (each isolate)

- The worker is spawned with no `execArgv` override, so it inherits
  `--expose-gc`.
- Probe op: one push (spinning on FULL) or one drain, while the peer keeps
  the lane moving.
- `measureOps{ ops: 10000, warmup: 2000, stabilize: true }`;
  `checkOps{ maxBytesPerOp: 8, maxMajorsPerKOp: 0 }`, per isolate.
- Reports over a dedicated MessageChannel port:
  `{ isolate, ran, gcFn, ops, bytesPerOp, verdict, gc }`.
- FAIL on any of: no report within 30 s; report count !== isolates spawned;
  `ran` false (an exception counts as `ran` false); `gcFn` false;
  `bytesPerOp` null or not finite; `verdict !== "pass"`.
- Gated PER ISOLATE. `aggregateWorkerReports` is REPORTED only: it dilutes
  (one isolate of 5 at 20 B/op averages 4 B/op and would pass).
- C9w runs in every isolate: an op that RETAINS one allocation per op (the
  T9 T6-control shape, `torture.mjs:645-659`; a transient allocation passes
  a `stabilize: true` gate) must not pass. L2 needs 5 reports and 5 C9w
  failures.
- Zero-box: probe counters live in an Int32Array or in local `|0` variables.

Deterministic: the 30 s limit is a no-report timeout, which is a FAIL, not a
measurement; the verdict is the retention gate's, on a fixed op count.

## T-05 -- Controls

SPLICE = a mutant of the serialized shipped body (D1); if the splice changes
nothing the control FAILs with "source drifted" (`torture.mjs:711`). MODEL = a
test-side broken writer through `mb-layout` against the REAL consumer,
checked by the shared predicate. Each racy control has a scripted twin (-s,
MODEL) gated every run, and a real-thread twin (-r) that gates only after
G4 (T-06). Per decisions/0002 Amendment A2 and the maintainer's Q3, there is
no `push(fill)` API: the scripted twins are MODEL only.

| Id | Injection | What it breaks | Predicate that fails | Tier |
|---|---|---|---|---|
| C1-s | MODEL | two writers interleave load, load, write, write, store, store | `oracleOk` (P1 missing 1 record) | DETERMINISTIC |
| C1-r | real threads | 2 writer workers on one lane, tight loop | `oracleOk` dup / missing | after G4 |
| C2-s | MODEL | slot pre-filled with a sentinel; writer stores TAIL, the real `drain()` runs, then the writer writes the payload | sentinel delivered (stamp / sentinel mismatch) | DETERMINISTIC |
| C2-r | SPLICE | TAIL store moved before the write, 1 worker | stamp / sentinel mismatch | after G4 |
| C3 | SPLICE | `(a-b)\|0` replaced by `<`, run in L3 | `oracleOk` (wrap) | DETERMINISTIC |
| C4 | SPLICE | the FULL return removed | `fullAcctOk` | DETERMINISTIC |
| C5-s | MODEL (M5) | racy MPMC claim interleaving | `oracleOk` (exactly-once results) | DETERMINISTIC |
| C5-r | SPLICE (M5) | load + store claim in L6, 4 x 4 | `oracleOk` dup / missing | after G4 |
| C6-s | MODEL | writer loads the bitmap word, the real drain frees a slot, the writer stores word\|bit; popcount ends at 1 | `slabRestOk` | DETERMINISTIC |
| C6-r | SPLICE | plain read-modify-write on the bitmap in L6 | `slabRestOk` / stamp / unreachable SLAB_EXHAUSTED | after G4 |
| C7 | MODEL (M5) | settling without dedupe + one injected duplicate settles twice; the positive half expects `dupes === 1` | `settledOnceOk` | DETERMINISTIC |
| C8 | MODEL | spoofed `lw:mb:sab` (plain ArrayBuffer, matching layout) leaves the consumer on the fallback; T9 shape, `torture.mjs:745-770` | mode === "transfer" | DETERMINISTIC |
| C9 | MODEL (T9 shape) | a drain on main that RETAINS one allocation per message (T9 T6-control shape, `torture.mjs:645-659`) must fail L9, plus C9w per isolate | the L9 gate (T-04) | DETERMINISTIC |
| C10-s | MODEL | writer reads ARMED before the real consumer arms, stores TAIL and skips notify; the stranded probe returns 1. Positive half: re-load after the TAIL store; probe returns 0 | `wakeOk` | DETERMINISTIC |
| C10-r | SPLICE | producer reuses the ARMED value from its FULL read; bursts of 1000, gated by an ack counter in the SAB | rescued wake: delivered < M at the 2 s deadline, === M after a rescue notify | after G4 |
| C11 | MODEL | the trace M-08 used to allow (CAP accepted, then FULL forever, no nak) | `handshakeOk` | DETERMINISTIC |

C1..C11 give 16 rows; C1, C2, C5, C6 and C10 each have -s and -r twins.
C3, C4, C7, C8, C9, C11 have no real-thread twin.

C5-r and C6-r are L6's contention witnesses (`RESEARCH.md` 4.4): a scripted
twin cannot replace them. Precedent, not a firing rate: `RESEARCH.md` 7.2's
racy control (3 producers x 500 k) delivered 228, with 38 violations and
1,499,772 missing (Apple M4 Pro, Node v26.8.2, load ~2.6-2.9, one run,
PRELIMINARY).

## T-06 -- G4 firing-rate protocol

The measurement is a mutant build, which is code: M3 measures C1, C2, C6 and
C10; M5 measures C5. This overrides decisions/0002 M-12's "M2 records"
(Amendment A1).

- N = 100 runs, each a fresh `node --expose-gc` process. Fixed work 3e5 ops
  with a 20 s cap; hitting the cap makes the run INVALID. The mutant AND the
  correct build both run.
- Fired = the targeted predicate FAILs: oracle dup / missing (C1, C5);
  stamp or sentinel mismatch (C2); mismatch, popcount != 0, or an
  unreachable SLAB_EXHAUSTED (C6); a rescued wake (C10): delivered < M at the
  2 s deadline and === M after a rescue notify.
- DETERMINISTIC needs all three: 100/100 fired on the mutant, 0/100 false
  fires on the correct build, 0 invalid runs. It then gates with the same
  parameters.
- If C1-r, C2-r or C10-r miss that, they become REPORTED and their -s twin
  gates.
- If C5-r or C6-r miss it, tune L6 for up to 3 rounds; if still short, they
  become STATISTICAL: K = 5 rounds, fired 5/5 passes, 1-4 fired exits 2
  (HOST, inconclusive), 0 fired fails.
- Expected (not measured): C1-r and C5-r fire 100/100; C2-r and C10-r do not
  (nanosecond windows); C6-r is uncertain (one allocator against one freer).
- Provenance per run set: date, host, cores, cache line, OS, Node / V8,
  loadavg at start and end, other busy node processes, lock status,
  `Worker.js` and mutant sha256, seeds, N, and per run
  `{ fired, signal, ops, ms }`.
- Invocation: `node ~/.claude/hooks/quiet-lock.mjs status` first, then
  `node ~/.claude/hooks/quiet-lock.mjs run "lite-worker M3 firing C1-r" -- node --expose-gc test/torture/mb-firing.mjs --control C1-r --runs 100`.
  Exit 75 = BUSY HOST: record PENDING in NEXT.md; never retry in a loop.
- Reference host (maintainer Q4): the package has no CI. The reference host
  is the maintainer's machine only (Apple M4 Pro per `RESEARCH.md` 7.2); the
  actual host is recorded in each run set's provenance.

## T-07 -- Rejected SAB and the pre-verdict rule

Decided by the maintainer 2026-10-18 (Q2a, Q2b); amends decisions/0002 M-04
and M-08 (Amendment A4).

- A mismatched `lw:mb:sab` gets an `lw:mb:nak` reply. In "shared" mode every
  push after the nak returns REJECTED, distinct from FULL and RETIRED. In
  "auto" mode delivery stays lossless on the fallback (mode "transfer", never
  REJECTED). `lw:mb:nak` and `lw:mb:ack` join the reserved `lw:mb:*`
  namespace.
- Pre-verdict pushes: the producer STARTS ON THE FALLBACK (transfer path) and
  switches to the SAB only after the consumer's ack. Nothing is re-sent;
  lossless by construction. Rejected: an optimistic SAB write with a re-send
  on nak.
- L8 carries one case per field: byteLength, CAP, W, TYPE, GEN.
  `handshakeOk` = no FULL after the nak, and accepted === delivered.
- C11 is the control: the trace the old M-08 allowed (CAP accepted, then FULL
  forever, no nak) must fail `handshakeOk`.

Open: the numeric encoding of REJECTED, and the `lw:mb:nak` / `lw:mb:ack`
payload fields, are M3's pick (as M-04 says for the other four outcomes).
Status: decided as Q6, see "M3 decisions".

## T-08 -- M3 B0 parity

The M3 coder does this before any `Worker.js` edit. Freeze
`git show HEAD:Worker.js` to a known folder. `parity-trace.mjs` records
`frameChannel` in both modes (read values, `dropped`, `free`, `byteLength`,
`mode`), the post / call `{ t, i, d }` envelopes and call resolutions, and the
export keys and typeof except VERSION. MP compares deep-equal against
`golden/parity-1.4.0.json`. Must-fail control: SPLICE the frozen copy's
`dropped` count. `_source` bytes are NOT compared: serializing the mailbox
body into WORKER_RUNTIME changes `_source` for every worker, so parity is
behavioral only.
Status: B0 froze the copy to `test/torture/frozen/Worker-1.4.0.js`
(committed), not a scratch folder; see "M3 decisions".

## Maintainer decisions (2026-10-18)

- **Q1.** Doc home is this file, Status: proposed.
- **Q2a.** YES: REJECTED is a 5th push outcome (T-07).
- **Q2b.** Pre-verdict pushes use the fallback until the ack (T-07).
- **Q3.** NO `push(fill)` API commitment. C1 / C2 / C10 scripted twins are
  MODEL only; the planner's conditional SPLICE twins are dropped.
- **Q4.** No CI; the reference host is the maintainer's machine only (T-06).
- **E2.** PASSED 2026-10-18: the maintainer read the license at the source
  (1024cores.net): Simplified (2-clause) BSD, (c) 2010-2011 Dmitry Vyukov,
  matching the mirror text in `RESEARCH.md`. Remaining M3 gate: the
  maintainer's explicit approval only.

## M3 decisions (2026-10-18, M3 planner pass)

Source: the M3 plan (planner pass, with orchestrator addenda AD1, AD2). They
decide T-00 D1-D4 and Open items Q5-Q8. Status of this record stays
proposed; none is code yet.

- **D1.** The factory is `makeMailbox(transport, layout, opts)`, module
  scope and self-contained. `buildSource` appends
  `var __makeMailbox = (...)` and `__ctx.mailbox = ...` after the canvas
  line (`Worker.js:535-536`). SPLICE anchors are literal statements that
  each occur exactly once in `_source`: A_PUBLISH (the TAIL store), A_FULL,
  A_WRAP (`((t - h) | 0)`), A_ARMED (re-load + notify), A_CLAIM (the slab
  CAS). B1 records the exact text of each.
- **D2 / Q8 (Int32 words).** Consumer line: HEAD 0, ARMED 1. Producer line:
  TAIL 32, FBN 33. Cold line from 64: GEN 64, STATE 65, CAP 66, W 67,
  TYPE 68, SLOTS 69, SLOT_BYTES 70. Payload starts at byte 384. Lane stride
  = 384 + roundUp128(CAP * W * elt) + a 128 B trailing GAP. The stride
  grows by the slab region of D3 when SLOTS > 0; B1 fixes the exact formula.
- **D3.** The slab sits inside its lane's stride: payload end + 128 GAP,
  then [HINT words][BITMAP words] (bit 1 = used; slot = 32w + k; HINT bit j
  = word j may be free), then roundUp128 + 128 GAP, then the slots
  (`SLOT_BYTES % 8 === 0`), then 128 GAP.
- **D4.** `typeof Atomics.waitAsync` is checked at construction. Each arm
  reads `Atomics.waitAsync` as a live property, never a cached reference.
- **Q5.** The L10 arm count stays single-thread (exactly R arms). Two
  variants are added: a worker producer and a worker consumer (waitAsync).
  Both gate `oracleOk` and `wakeOk`; their arm counts are REPORTED. L4 has
  no worker variant.
- **Q6.** `push` returns a Smi: OK 0, FULL 1, SLAB_EXHAUSTED 2,
  SLOT_TOO_SMALL 3, RETIRED 4, REJECTED 5. The codes are statics on
  `mailbox`, not new exports. Zero-box: `push(src, off)` takes a typed
  array and a Smi offset. `lw:mb:nak {id, gen, why}`: why 1 byteLength,
  2 CAP, 3 W, 4 TYPE, 5 GEN, 6 not a SAB. One `lw:mb:ack {id, gen, credit,
  attached}` carries both the M-08 credit and the A4 verdict. FIFO across
  the fallback -> SAB switch: the producer stores its fallback count in FBN
  before its first SAB TAIL store; the consumer reads the SAB only after it
  has delivered FBN fallback records.
- **Q7.** Reuse the harness xorshift32 (`test/torture/harness.mjs:114`).
  SEED = TORTURE_SEED, default 0x1a2b3c4d. Independent op and key streams
  per lane; salts are the lane number in hex (AD1): L1 SEED ^ 0x6d620101
  (op) / 0x6d620102 (key); L4 0x6d620401 / 0x6d620402; L6 0x6d620601 /
  0x6d620602. Coverage floor: every op kind >= 1000 per run and every
  outcome a lane can return >= 1, else FAIL.
- **B0 note.** B0 changes nothing in `Worker.js`. The frozen copy is
  `test/torture/frozen/Worker-1.4.0.js` (committed; sha256 equals
  `git show HEAD:Worker.js`; AD2 pins the b595f9e build, sha256
  b1ddbdf8412da33ed04af6b7ed7970a9a3c2bf127c14e60d0c67cf9e4938f726). The
  golden is `test/torture/golden/parity-1.4.0.json` with schema
  `{schema: 1, frozenSha256, trace}`. New export keys and handle-prototype
  names pass MP only if listed in ALLOWED_NEW (`[]` at B0; B1 adds
  `mailbox`). The must-fail control replaces `dropped++;` (it occurs
  exactly once, `Worker.js:342`) with an empty statement `;` in the frozen
  text (deleting it would make `transport.send(display);` the body of the
  `if`); an anchor count other than 1
  fails "source drifted", a mutant equal to the golden fails "decorative".

Deltas to decisions/0002 M-03 (explicit, not silent):

- ARMED is word 1, in the consumer line with HEAD (M-03 left the word to
  M3).
- FBN is word 33, in the producer line with TAIL (new; M-03 lists only
  TAIL). The producer writes it, so the line stays producer-only.
- The cold line gains SLOTS (69) and SLOT_BYTES (70) after TYPE.
- Lane stride gains a 128 B trailing GAP and holds the slab (D3); it is
  still a multiple of 128 B, so M-03's "rounded up to 128 B" holds.
- Push outcomes are numbered (Q6); M-04 left the encoding to M3.

Maintainer rulings (2026-10-18):

- **Merged ack: DECIDED (accepted).** One `lw:mb:ack {id, gen, credit,
  attached}`. The first ack is the A4 verdict and carries credit 0; later
  acks carry the M-08 credit. Amends decisions/0002:209-211 (M-08, one
  credit ack per drain) and decisions/0002:392-395 (A4, ack as verdict).
- **FBN FIFO rule: DECIDED (accepted).** The producer stores its
  fallback-record count in FBN (word 33) before its first SAB TAIL store.
  The consumer reads the SAB only after it has delivered FBN fallback
  records. Nothing is re-sent (Q2b). Amends decisions/0002 A4 (the switch)
  and T-07 above, which said only "nothing is re-sent".

No other pick contradicts 0002 M-03..M-09 or A1-A6.

## Open items (the plan is silent; not decided here)

Q5-Q8 are decided: see "M3 decisions" above. The original text is kept.

- **Q5.** Thread count for L10's wake-up bursts beyond "single thread" for
  the arm count; and whether L4 / L10 each need a worker variant.
- **Q6.** The numeric encoding of REJECTED and the nak / ack payloads (T-07).
- **Q7.** The seeds and PRNG for any randomized op stream (L1, L4 mixed
  ops, L6) and the asserted coverage floor; the suite rule wants a seeded
  32-bit PRNG with independent key and op streams, which M3's coder chooses.
- **Q8.** The ARMED word index (D2) and the slab bitmap layout (D3).

## Consequences

- M2 changes only documents: this file, an Amendments section and three
  in-place E2 status lines in decisions/0002, ROADMAP.md and RESEARCH.md.
  `Worker.js`, `Worker.d.ts`, `test/`, README, llms.txt and CHANGELOG are
  untouched; VERSION is untouched.
- M3 inherits a written oracle, 11 lane rows, controls C1-C11 with -s / -r
  twins, and five dependencies (D1-D5) it must satisfy or report.
- A real-thread control gates in the DETERMINISTIC tier only after G4 shows
  100/100 fired, 0/100 false fires, 0 invalid (T-06); until then its
  scripted twin carries the gate.
- Firing-rate numbers enter docs only from an M3 / M5 run log with the
  provenance in T-06.
