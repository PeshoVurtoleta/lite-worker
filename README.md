# @zakkster/lite-worker

[![npm version](https://img.shields.io/npm/v/@zakkster/lite-worker.svg?style=for-the-badge&color=latest)](https://www.npmjs.com/package/@zakkster/lite-worker)
[![sponsor](https://img.shields.io/badge/sponsor-PeshoVurtoleta-ea4aaa.svg?logo=github)](https://github.com/sponsors/PeshoVurtoleta)
![Zero-GC](https://img.shields.io/badge/Zero--GC-Engine-00C853?style=for-the-badge&logo=leaf&logoColor=white)
[![npm bundle size](https://img.shields.io/bundlephobia/minzip/@zakkster/lite-worker?style=for-the-badge)](https://bundlephobia.com/result?p=@zakkster/lite-worker)
[![npm downloads](https://img.shields.io/npm/dm/@zakkster/lite-worker?style=for-the-badge&color=blue)](https://www.npmjs.com/package/@zakkster/lite-worker)
[![npm total downloads](https://img.shields.io/npm/dt/@zakkster/lite-worker?style=for-the-badge&color=blue)](https://www.npmjs.com/package/@zakkster/lite-worker)
![Tree-Shakeable](https://img.shields.io/badge/tree--shakeable-yes-brightgreen)
![TypeScript](https://img.shields.io/badge/TypeScript-Types-informational)
![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)
[![license](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](./LICENSE.txt)

Zero-GC per-frame worker channel. Define a worker **inline from a function**, message it with **transferables**, and keep the main thread clean at 60fps. Zero dependencies, single file, full `.d.ts`.

## Positioning

**Not a Comlink competitor.** Comlink is Proxy-based RPC that allocates per call — fine for request/response, wrong for a 60fps loop. `lite-worker` is the zero-GC per-frame channel: the main thread stays clean for rendering, the sim ticks off-thread, and nothing allocates in steady state.

One worker uses one extra core; the value is **a clean main thread, not parallelism**. (Parallelism is `lite-worker-pool`'s job — see v2.0.0.)

## Install

```
npm i @zakkster/lite-worker
```

## The two transports

One `Worker`, two channels, chosen by what the payload is:

| | API | Envelope | Allocation | Use for |
|---|---|---|---|---|
| **raw** | `send()` / `onRaw()` | none — the buffer *is* the message | zero in steady state | 60fps traffic: sim frames, sample blocks, pixel buffers |
| **typed** | `post()` / `call()` / `on()` | `{t,i,d}` | small, only on `call` bookkeeping | control plane: init, config, request/response |

Discrimination is a single `instanceof` check on receipt — an `ArrayBuffer`/view goes to `onRaw`, anything else is an envelope. No allocation to tell them apart.

Why raw is actually zero-GC: transferred `ArrayBuffer`s are *moved*, not structured-cloned, so nothing is copied; and because there's no wrapper object, nothing is allocated to carry them. `send()` reuses a single-slot transfer list internally, so the send path itself allocates nothing per frame. The typed `post()` path reuses one scratch envelope for the same reason — `postMessage` snapshots synchronously, so reuse is safe.

## Quick start

```js
import { defineWorker } from "@zakkster/lite-worker";

// The body is serialized to a Blob URL. It is self-contained: it cannot close
// over anything from this module. It receives its channel as `ctx`.
const sim = defineWorker((ctx) => {
  let state = new Float64Array(1024);

  // control plane
  ctx.on("configure", (cfg) => { /* ... */ });

  // request/response — reply() may transfer
  ctx.on("snapshot", (_, reply) => reply(state.buffer, [state.buffer]));

  // hot path: fill an incoming buffer and transfer it straight back
  ctx.onRaw((buf) => {
    const f = new Float64Array(buf);
    for (let i = 0; i < f.length; i++) f[i] = Math.sin(i);
    ctx.send(f); // buffer auto-transferred
  });
});

sim.spawn();

// per-frame: transfer a buffer in, get one back — no allocation on the main thread
sim.onRaw((buf) => { /* draw from buf, then send it back next frame */ });
sim.send(new Float64Array(1024));

// occasional control/RPC
sim.post("configure", { seed: 42 });
const snap = await sim.call("snapshot");

sim.terminate();  // reusable — spawn() again to restart
sim.destroy();    // idempotent final teardown
```

## Ownership: the ping-pong

`send()` **transfers** the buffer, so after `send(buf)` your `buf` is detached — you no longer own it. The worker fills it and transfers it back to your `onRaw`. Hold two buffers and ping-pong them (one in flight, one being drawn) to keep both threads busy without allocating. See `demos/oscilloscope.html` for a working pool.

## API

### `defineWorker(moduleFn, options?) → WorkerHandle`

Serializes `moduleFn` into a Blob URL. The worker is created lazily on `spawn()`.

- `moduleFn(ctx)` — self-contained; **cannot** reference outer scope or use bare `import`.
- `options.type` — `"classic"` (default, widest support) or `"module"` (ESM inside the worker; needs a browser that supports module workers from Blob URLs).
- `options.name` — devtools label.
- `options.onError(err)` — uncaught worker errors (also emitted as the `"error"` event).

### `WorkerHandle` (main thread)

- `spawn(): this` — create the Worker. Idempotent while spawned; throws if destroyed.
- `send(buffer, transfer?)` — raw transfer. Buffer auto-transferred when `transfer` is omitted. No-op before spawn / after terminate.
- `onRaw(fn) → off` / `offRaw(fn)` — raw buffers from the worker.
- `post(type, data?, transfer?)` — fire-and-forget typed message.
- `call(type, data?, { transfer?, timeout? }?) → Promise` — request/response. Rejects on handler throw, missing handler, timeout, or terminate. Allocates — don't use per frame.
- `on(type, fn) → off` / `off(type, fn)` — typed pushes from the worker.
- `terminate(): this` — stop the worker, reject pending calls; handle stays re-spawnable.
- `destroy()` — idempotent full teardown; handle becomes unusable.
- `spawned` / `destroyed` — booleans.

### `ctx` (worker side)

Symmetric: `on(type, (data, reply?) => …)`, `off`, `onRaw`, `offRaw`, `post`, `send`, `close()`.

For a `call`, either invoke `reply(data, transfer?)` or return a value / resolving promise (auto-replies when `reply` wasn't called). Throwing rejects the caller's promise.

## frameChannel — bounded, latest-wins frame passing (v1.1.0)

The ping-pong above is the right pattern, but you shouldn't have to hand-roll the buffer accounting. `frameChannel` does it: a fixed pool of pre-allocated ArrayBuffers cycles producer→consumer and back via transfer, with **latest-wins backpressure**. If the consumer falls behind, intermediate frames drop instead of queueing — memory is bounded by the pool and cannot grow.

```js
// worker side (the producer): free-run a sim, publish frames
const sim = defineWorker((ctx) => {
  const N = 1024;
  const ch = ctx.frameChannel(1, { role: "producer", capacity: N }); // stride 1, Float32
  let phase = 0;
  setInterval(() => {
    phase += 0.06;
    // produce() returns false when the pool is momentarily exhausted — the frame
    // is dropped (latest-wins) and the sim just advances. It never blocks or queues.
    ch.produce((s) => { for (let i = 0; i < N; i++) s[i] = Math.sin(i / N * 6.283 * 4 + phase); });
  }, 8);
}).spawn();

// main side (the consumer): draw the freshest frame each rAF
const scope = sim.frameChannel(1, { role: "consumer", capacity: 1024 });
function draw() {
  const frame = scope.read();   // Float32Array | null — cached, allocation-free
  if (frame) { /* ...draw frame... */ }
  requestAnimationFrame(draw);
}
requestAnimationFrame(draw);
```

**Layout.** A number is a Float32 stride (floats per instance), `{ stride, capacity }` is the same but self-contained, and `{ bytes }` is a generic byte buffer. The stride form mirrors lite-gl's `LAYOUT` (`POINT: 8`, `QUAD`/`LINE: 9`): a channel built with `{ stride: LAYOUT.POINT, capacity: N }` holds a `Float32Array` of exactly `capacity * stride`, so a projected instance field moves across the worker boundary unmodified — same interleaving, same `i * stride` indexing, no repack.

**Pool size.** `count` (default 2) is the number of buffers that ping-pong. Two is a double buffer (one drawn, one filled); three gives the producer a deeper pipeline before it starts dropping. Memory is exactly `count` buffers, forever.

**API.** Producer: `produce(fill) → boolean` (false = dropped), `free`, `dispose()`. Consumer: `read() → view | null`, `hasNew`, `dropped`, `dispose()`. Both expose `stride`, `capacity`, `byteLength`, `count`, `kind`, `role`.

**Honest allocation note.** The data path is genuinely zero-copy (buffers are transferred, never cloned) and envelope-free (a raw buffer arriving at the consumer *is* a frame; arriving at the producer *is* a recycle — direction carries the meaning, so there's no tag object). The one unavoidable cost on the transfer model is a single typed-array *header* per buffer hop: transfer hands each side a fresh `ArrayBuffer` identity, so the view can't be cached across the boundary. `read()` itself is allocation-free (the view is cached until the next frame swaps in). That header is a few dozen bytes of transient garbage per hop — not a data copy — so throughput and memory stay flat (see `bench/frame-soak.mjs`). A `SharedArrayBuffer`-backed mode with literally zero per-hop headers is the natural next step, at the cost of requiring COOP/COEP — see the roadmap.

**One channel owns the raw stream.** A `frameChannel` takes over `send`/`onRaw` on its transport; don't also use them directly while it's live. The typed plane (`post`/`call`/`on`) stays free for control messages like config.

## Constraints & gotchas

- **Self-contained body.** The function is `.toString()`-serialized; it cannot capture variables from the surrounding module. Pass everything in via `post`/`send`, or inline it in the body.
- **CSP.** Blob workers need `worker-src blob:` (or `child-src blob:` on older policies) in your Content-Security-Policy.
- **Transferables, not SharedArrayBuffer.** v1.0.0's raw transport is built on transfer semantics and needs **no** cross-origin isolation (no COOP/COEP). Raw payloads must be `ArrayBuffer`s or ArrayBuffer views; a `SharedArrayBuffer` is out of scope here.
- **No Blob URL leak.** The object URL is revoked the instant the `Worker` is constructed — the browser has already fetched the script by then. Spinning up many short-lived workers without ever calling `destroy()` leaves no object URLs piling up for the document's lifetime. `destroy()` is still the right call to free the worker *thread*; it just isn't what frees the URL.
- **One extra core, not N.** A single worker is about keeping the main thread clean. For fan-out across cores, that's `lite-worker-pool` (v2.0.0).

## Roadmap

- **v1.0.0** — inline worker core: `defineWorker`, typed request/response with transferables, `spawn()` / `terminate()` / idempotent `destroy()`.
- **v1.1.0** (this release) — `frameChannel`: bounded, latest-wins frame passing over a fixed transfer pool, with lite-gl `LAYOUT`-compatible strides.
- **v1.2.0** (planned) — `SharedArrayBuffer`-backed frame channel: literally zero per-hop headers and stable views on both sides, gated behind cross-origin isolation (COOP/COEP).
- **v2.0.0** — `lite-worker-pool`: parallelism across cores, work-stealing, the same zero-GC channel per lane.

See [`CHANGELOG.md`](./CHANGELOG.md) for the full release history and [`llms.txt`](./llms.txt) for a machine-readable API digest.

## License

MIT © Zahary Shinikchiev
