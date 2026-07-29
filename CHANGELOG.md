# Changelog

All notable changes to `@zakkster/lite-worker` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.3.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.3.0
[1.2.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.2.0
[1.1.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.1.0
[1.0.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.0.0
