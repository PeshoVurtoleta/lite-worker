# Changelog

All notable changes to `@zakkster/lite-worker` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.3] - 2026-08-09

Contract release. Pins the two unstated rules W1's proof surfaced: the live-view
lifetime a renderer may rely on (W-06) and the trust the SAB handshake extends to
an incoming typed message (W-10). The only `Worker.js` logic change is one clause
in the one-time `lw:fc:sab` handshake handler; the `produce`/`read`/`readInto`
hot bodies are byte-for-byte unchanged.

### Added

- **Torture T4 -- adversarial backpressure.** Producer:consumer ratios 1:1,
  1000:1, 1:1000; bursty bursts; `count` 2/3/8; a `fill` that throws (the pool
  stays intact and the buffer returns to `free`); dispose mid-flight from each
  side. After each run the conservation invariant holds and `produced + dropped`
  accounts for every tick. Runs over the in-process loopback transport (no real
  thread needed).
- **Torture T8 -- lite-gl LAYOUT stride conformance.** Local stride constants
  mirroring lite-gl (POINT 8, QUAD/LINE 9 -- no runtime dependency) drive a
  channel; asserts `view.length === capacity * stride`, that `i * stride`
  indexing survives the channel boundary unmodified, and that a projected
  instance field round-trips bit-for-bit in BOTH transfer and shared mode.
- **Torture T9 controls for T4/T8.** A spoofed-handshake control (a plain
  `ArrayBuffer` posted as `lw:fc:sab` with matching byteLength/count) proves the
  W-10 guard is what blocks the attach -- without it the same message would
  upgrade the consumer to shared. A T4 conservation control leaks a buffer per
  hop and MUST fail the conservation assertion. T4/T8 flip from `pending` to
  `run`.
- **`decisions/0001-frame-channel-contract.md`.** Records W-06 (document the
  live-view boundary as inherent to a fixed pool; do NOT add a `count`-aware
  helper) and W-10 (handshake fails closed on a non-SharedArrayBuffer; `lw:fc:*`
  is a reserved namespace).

### Changed

- **W-10 -- handshake fails closed.** The `lw:fc:sab` handler now rejects any
  `sab` that is not a real `SharedArrayBuffer`, so a spoofed handshake with
  matching byteLength/count but a plain `ArrayBuffer` leaves the consumer on the
  transferable ring (`mode === "transfer"`, `shared === false`). The distinct
  layout-mismatch return stays on its own line.

### Documentation

- **W-06 -- live-view boundary documented.** `Worker.d.ts`, `README.md`, and
  `llms.txt` now state that `read()`'s view is valid for exactly `count - 1`
  further `produce()` calls and is overwritten on the next, and direct holders to
  `readInto` for retention across a yield. `lw:fc:sab`/`lw:fc:hello` are now
  called out as a reserved namespace ("don't send them yourself"), matching the
  `lw:canvas*` note.

### Deferred

- W-07 and the doc allocation-number regeneration remain deferred to W3.

## [1.3.2] - 2026-08-09

Proof release. No runtime behaviour changes to `Worker.js` -- only the
three-place `VERSION` bump. Shared mode's tear-free seqlock is now proven against
a REAL hostile writer on a second OS thread, converting W-04/W-05 from a green
light over a hole (blueprint AR-02) into a proof.

### Added

- **Torture T3 -- real two-thread SAB seqlock (W-04/W-05).** A real
  `node:worker_threads` Worker evaluates lite-worker's serialized `WORKER_RUNTIME`
  + `makeFrameChannel` UNCHANGED (via the real serializer, over a `parentPort`
  `self`-shim) and publishes frames in a tight hostile loop; every lane carries a
  monotonically-increasing frame id, so a torn read shows a lane that disagrees.
  The main thread hammers `read()`/`readInto()`. Over >= 1e6 hostile ops:
  `readInto()` leaked **zero** torn frames (every returned snapshot is a single
  coherent frame the producer actually wrote), and `torn` is provably **> 0**
  (~1.2M-1.4M observed) -- the metamorphic opposite of W-05's structural zero.
  Includes a light W-06 live-view boundary spot-check (coherent for exactly
  `count - 1` publishes; full pinning is deferred to W2) and the T6 retention
  gate run over the real thread in both modes (shared via `measureOps`, ~0 B/op;
  transfer via a post-`settle()` heap delta, since the async transfer cannot fit
  `measureOps`'s synchronous op model).
- **Torture T5 -- differential fuzz across a real thread.** Mixed
  produce/read/readInto ops (100k) driven lockstep against a plain-array oracle
  of the newest value published; every read must equal the oracle's newest
  (latest-wins drops allowed). Divergence prints `TORTURE_SEED` + op index for
  replay.
- **Torture T9 seqlock control.** A copy of the serialized producer with the
  odd/even `SEQ` bumps stripped is run as a hostile writer; the all-lanes-equal
  tear predicate that T3/T5 rely on MUST catch the resulting torn reads (~4k
  observed). A tear gate that cannot fail is decorative -- this proves it can.
- `test/torture/thread-entry.mjs` and a `nodeThreadTransport` harness helper wire
  the real-thread transport. `test/` stays out of `files[]`.

### Known issues (still deferred)

- **W-06** -- the shared-mode live-view lifetime is spot-checked in T3 but not yet
  pinned by name in both modes with docs; that is W2.
- **W-07** -- README/llms.txt reference a `demos/` path (dir is `demo/`); fixed in
  W3 with the doc-number regeneration.
- **W-10** -- the `lw:fc:sab` handshake does not yet guard
  `m.sab instanceof SharedArrayBuffer`, and `lw:fc:*` is undocumented as reserved;
  hardened + documented in W2.

## [1.3.1] - 2026-08-09

Proof-infrastructure patch release. No runtime behaviour changes to `Worker.js` --
only enforcement, tests, and packaging so the guarantees are provable.

### Fixed

- **The release gate had never run.** `bench/gc-gate.mjs` imported `defineWorker`/
  `frameChannel` from `../Worker.d.ts` (the declaration file) instead of
  `../Worker.js`, so `npm run gate` threw `SyntaxError` and no gate output was
  ever produced. Fixed the import; `npm run gate` now runs green and prints
  actual numbers. (W-01)

### Added

- **`VERSION` export** in `Worker.js` (and declared in `Worker.d.ts`), value
  `"1.3.1"`. Establishes three-place version sync (package.json + `VERSION` +
  CHANGELOG) from this release forward. A test asserts `VERSION` equals the
  package.json version. (W-03)
- **`test/torture.mjs` + `test/torture/harness.mjs`** -- a tiered torture suite
  gated with `@zakkster/lite-gc-profiler` (zero-retention) and
  `@zakkster/lite-leak` (soak/retention). Tiers wired now: T0 channel laws
  (latest-wins, bounded, pool conservation), T1 degenerate layouts/payloads, T2
  lifecycle abuse (incl. 4096 spawn/destroy cycles with zero retained Blob URLs
  and zero live workers), T6 zero-retention gate (both modes, `maxBytesPerOp:8`,
  `maxMajorsPerKOp:0`) plus pool `byteLength` invariance, T7 soak (4096 cycles +
  conservation invariant + lite-leak churn), and T9 controls (each gate has a
  deliberately-broken variant that must be caught). T3/T4/T5/T8 are registered as
  pending placeholders for later sessions; the harness includes a
  `worker_threads` `self`-shim scaffold for the real-thread seqlock work. Run
  with `npm run torture`. (W-02)
- `"bugs"` field in package.json pointing at the issue tracker. (W-09)

### Changed

- **Test runner ported to `node:test`.** `test/Worker.test.js` was a bespoke
  `ok()`/`process.exit()` runner; it now uses `node:test` + `node:assert/strict`
  with every original assertion preserved, and `npm test` is `node --test`. (W-08)
- Dev dependencies: `@zakkster/lite-gc-profiler` moved to `^1.11.0`;
  `@zakkster/lite-leak` (`^1.8.1`) added, as required by the torture harness. (W-09)

### Known issues (fixed downstream)

These findings are recorded here and addressed in W1-W3:

- **W-04 / W-05** -- shared-mode's tear-free seqlock is only exercised against a
  synchronous producer, so the retry branches are never taken and `torn` is
  structurally 0. A real `worker_threads` hostile-writer proof lands in W1
  (torture T3/T5).
- **W-06** -- the live-view lifetime in shared mode (a `read()` view is valid for
  exactly `count - 1` subsequent publishes) is documented loosely but not pinned
  by a named test. Pinned in W2.
- **W-10** -- the `lw:fc:sab` handshake validates `byteLength`/`count` but not
  `m.sab instanceof SharedArrayBuffer`, and the `lw:fc:*` reserved namespace is
  undocumented. Hardened + documented in W2.
- **W-07** -- README/llms.txt reference a `demos/` path; the directory is `demo/`.
  Fixed in W3 alongside the doc-number regeneration.

## [1.3.0] - 2026-07-19

### Added

- **Shared mode** — `frameChannel` can now put frames in a `SharedArrayBuffer`
  and publish them with an `Atomics` seqlock (bump the sequence odd, swing the
  published-slot index, bump it even). The producer writes one slot while the
  consumer reads another, so in steady state there is **no `postMessage` traffic
  on the data path at all**.
  - `options.mode`: `"auto"` (default — take the shared path when it's available,
    otherwise the transferable ring), `"shared"` (alias `"sab"` — require it, and
    throw rather than degrade), or `"transfer"` (always the ring).
  - Negotiated automatically: the producer allocates the SAB and hands it over
    through a reserved `lw:fc:sab` typed message; the consumer starts on the ring
    and upgrades in place. Construction order doesn't matter — a consumer built
    later announces itself via `lw:fc:hello` and the producer re-sends. A
    handshake whose layout doesn't match is ignored.
  - Falls back transparently when `SharedArrayBuffer` is missing, the page isn't
    cross-origin isolated (`crossOriginIsolated === false`), or the transport has
    no typed plane. **The transferable ring stays the default.**
  - `channel.mode` / `channel.shared` report which transport is in use.
- `consumer.readInto(dst)` — copies the freshest frame under the seqlock and
  retries if a publish lands mid-copy, for when the data outlives the read.
  `consumer.torn` counts those retries. `read()` remains the allocation-free fast
  path and, in shared mode, returns a view onto live shared memory.
- `producer.published` (shared mode) — total frames published.
- `bench/gc-gate.mjs` (`npm run gate`) — the release gates, measured with
  `@zakkster/lite-gc-profiler`: a 10k-frame steady-state retention soak on the
  frame channel in both directions and both modes, a 200-cycle spawn/destroy
  orphan check (Blob URLs, worker threads), and a bundle guard (single JS file,
  size budget, no runtime dependencies).

### Notes

- Shared mode's measured *retention* is not lower than the ring's: the ring's
  per-hop view header is transient garbage, not retained memory, so a
  surviving-allocation gate reads ~0 for both. What shared mode actually buys is
  throughput (~1.4-2.1x across repeat gate runs) and the removal of message traffic.
- Cross-origin isolation is a real deployment cost (`COOP: same-origin` +
  `COEP: require-corp`, and cross-origin embeds must send CORP/CORS). Shared mode
  is opt-in by design and never silently changes what a non-isolated page does.

## [1.2.0] - 2026-07-17

### Added

- `handle.adoptCanvas(canvasEl, options?)` — `transferControlToOffscreen()` plus
  the two things that are easy to forget: **resize forwarding** (a `ResizeObserver`
  on the main-thread element posts device-pixel dimensions + `dpr` to the worker)
  and **visibility forwarding** (`visibilitychange` is posted so the worker's
  render loop auto-pauses when the tab is hidden). Returns a controller with
  `resize()`, `pause()`, `resume()`, and `dispose()`. Throws if the canvas does
  not support `transferControlToOffscreen`.
- `ctx.onCanvas(cb, options?)` — worker-side counterpart. The callback receives
  the transferred `OffscreenCanvas` and a `CanvasControl` with `width`/`height`/
  `dpr`/`visible`, `onResize()`, `onVisibility()`, and `frame(fn, { fps? })` — a
  render loop that prefers `requestAnimationFrame` (exposed on the worker global
  alongside OffscreenCanvas) for vsync pacing, falls back to a timer when it's
  absent or when a rate is given, and auto-pauses when the owning tab is hidden.
  The callback receives a delta time in ms.
- Reserved `lw:canvas*` typed message types carry the adoption protocol. Like
  `frameChannel`, the worker-side implementation is serialized from the single
  main-side definition, so the two sides share one source of truth.
- Types: `AdoptCanvasOptions`, `CanvasAdoption`, `CanvasControl`, `CanvasFrameOptions`.
- Tests: adoptCanvas/onCanvas protocol (adoption dims, resize + visibility
  forwarding, the render loop over both the rAF and timer paths), and a
  spawn/destroy leak gate asserting no orphaned Blob URLs across 50 cycles.

### Fixed

- A worker error (a load/parse failure or an uncaught error) now rejects all
  in-flight `call()` promises immediately instead of letting them hang until
  their timeout. Handler-level throws during a call are still replied to
  individually.

### Changed

- The oscilloscope demo's OffscreenCanvas scene now uses `adoptCanvas`/`onCanvas`
  instead of hand-rolled canvas transfer, resize, and visibility wiring, and its
  synth advances phase by real elapsed time (delta-time) so the waveform's
  frequency is correct regardless of timer jitter or a backgrounded tab.

## [1.1.0] - 2026-07-16

### Added

- `frameChannel(transport, layout, options)` — a bounded, latest-wins frame
  channel over the raw transport, plus `handle.frameChannel(layout, options)` and
  worker-side `ctx.frameChannel(layout, options)`. A fixed pool of pre-allocated
  ArrayBuffers (default 2) ping-pongs producer→consumer and back via ownership
  transfer: no per-frame data allocation, no object clone, and memory that cannot
  grow. When the consumer falls behind, intermediate frames drop (latest-wins)
  rather than queueing.
  - Producer: `produce(fill) → boolean` (false = dropped), `free`, `dispose()`.
  - Consumer: `read() → view | null` (allocation-free; view cached until swap),
    `hasNew`, `dropped`, `dispose()`.
  - Layout is a Float32 stride, `{ stride, capacity }`, or `{ bytes }`. The stride
    form matches lite-gl `LAYOUT` (POINT 8, QUAD/LINE 9) so a projected instance
    field crosses the worker boundary unmodified.
  - The frameChannel implementation is defined once and serialized into the
    worker runtime, so the two sides share a single source of truth.
- `bench/frame-soak.mjs` — a fast-producer/slow-consumer soak that demonstrates
  the two guarantees empirically: the in-flight queue never exceeds the pool
  (bounded), and the heap does not grow across 100k hops (no leak).
- Types for `frameChannel`, `FrameLayout`, `FrameChannelOptions`,
  `ProducerFrameChannel`, `ConsumerFrameChannel`, and `RawTransport`.

### Notes

- A `frameChannel` takes ownership of the raw `send`/`onRaw` stream on its
  transport; the typed `post`/`call`/`on` plane remains free for control messages.
- The data path is zero-copy and envelope-free; the one cost on the transfer
  model is a single typed-array header per buffer hop (transient, not a copy). A
  `SharedArrayBuffer`-backed zero-header mode is planned for v1.2.0.

## [1.0.0] - 2026-07-16

Initial release — the inline worker core.

### Added

- `defineWorker(moduleFn, options?)` — build a Worker from a self-contained
  function via a Blob URL, with no separate file and no bundler configuration.
  Named export plus default export.
- Two transports over one Worker, routed by a single `instanceof` check with no
  allocation to discriminate:
  - **raw** (`send()` / `onRaw()`): zero-GC hot path for 60fps traffic. Transfers
    `ArrayBuffer`s instead of cloning; reuses a single-slot transfer list so the
    send path allocates nothing per frame. Buffers auto-transfer when no explicit
    transfer list is given.
  - **typed** (`post()` / `call()` / `on()`): `{t,i,d}` control-plane envelope for
    init, config, and request/response. `post()` reuses one scratch envelope;
    `call()` returns a Promise with correlation-id bookkeeping and an optional
    per-call `timeout`.
- Worker-side `ctx` mirroring the main side: `on`/`off`, `onRaw`/`offRaw`,
  `post`, `send`, `close`. `call` handlers may `reply(data, transfer?)` explicitly
  or return a value / resolving promise to auto-reply; throwing rejects the caller.
- Lifecycle: idempotent `spawn()`, re-spawnable `terminate()` that rejects pending
  calls, and idempotent `destroy()` for final teardown. `post`/`send` are no-ops
  after terminate; `call` rejects after terminate/destroy.
- `options.type` (`"classic"` default / `"module"`), `options.name`, and
  `options.onError` plus an `"error"` event for uncaught worker errors.
- Immediate Blob URL revocation: the object URL is revoked as soon as the Worker
  is constructed, so short-lived workers never leave object URLs accumulating for
  the document's lifetime.
- Full strict TypeScript declarations (`Worker.d.ts`).
- Node loopback test suite (22 cases) exercising both the main-side handle and the
  serialized worker-side runtime.
- Phosphor oscilloscope demo: off-thread waveform synthesis with double-buffered
  transferable ping-pong and a zero-allocation render loop.

[1.3.2]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.3.2
[1.3.1]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.3.1
[1.3.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.3.0
[1.2.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.2.0
[1.1.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.1.0
[1.0.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.0.0
