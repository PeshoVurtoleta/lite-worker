# Changelog

All notable changes to `@zakkster/lite-worker` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.1.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.1.0
[1.0.0]: https://github.com/PeshoVurtoleta/lite-worker/releases/tag/v1.0.0
