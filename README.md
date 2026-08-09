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

**Honest allocation note.** The data path is genuinely zero-copy (buffers are transferred, never cloned) and envelope-free (a raw buffer arriving at the consumer *is* a frame; arriving at the producer *is* a recycle — direction carries the meaning, so there's no tag object). The one unavoidable cost on the transfer model is a single typed-array *header* per buffer hop: transfer hands each side a fresh `ArrayBuffer` identity, so the view can't be cached across the boundary. `read()` itself is allocation-free (the view is cached until the next frame swaps in). That header is a few dozen bytes of transient garbage per hop — not a data copy — so throughput and memory stay flat (see `bench/frame-soak.mjs`). A `SharedArrayBuffer`-backed mode that removes the per-hop header entirely landed in v1.3.0 — see [Shared mode](#shared-mode--the-sharedarraybuffer-fast-path-v130). Worth knowing before you reach for it: those headers are transient garbage rather than retained memory, so the measured retention is the same either way; what shared mode actually removes is the message traffic.

**One channel owns the raw stream.** A `frameChannel` takes over `send`/`onRaw` on its transport; don't also use them directly while it's live. The typed plane (`post`/`call`/`on`) stays free for control messages like config.

**OffscreenCanvas is the complement, not a competitor.** If your only consumer is a canvas, `transferControlToOffscreen()` hands it to the worker and the worker draws its own frames — the data never crosses back, so you don't need a channel at all. `frameChannel` is for when the frames *must* reach another thread: the main thread (for DOM or main-only APIs) or a second worker. The demo shows both side by side — tab one draws on the main thread through `frameChannel`, tab two transfers the canvas and draws on the worker with the main thread idle.

## Shared mode — the SharedArrayBuffer fast path (v1.3.0)

The transferable ring is the default and always works. Where the page is **cross-origin isolated**, `frameChannel` can instead put the frames in a `SharedArrayBuffer` and publish them with an `Atomics` seqlock — the sim writes one slot while the consumer reads another, and the flip is three atomic ops. In steady state there is **no `postMessage` traffic on the data path at all**.

```js
// Nothing changes at the call site — mode defaults to "auto".
const ch = ctx.frameChannel({ stride: 8, capacity: 4096 }, { role: "producer" });
ch.mode;    // "shared" when it negotiated the SAB, "transfer" otherwise
```

- **It negotiates itself.** The producer allocates the SAB and hands it over once through a reserved typed message; the consumer starts on the transferable ring and upgrades in place when it arrives. Construction order doesn't matter (a late consumer announces itself and the producer re-sends), and if anything is missing — no `SharedArrayBuffer`, no isolation, or a transport without the typed plane — both sides just stay on the ring. **The ring is the default; the SAB is the earned upgrade.**
- **The deployment tax is real.** `SharedArrayBuffer` requires cross-origin isolation, which means serving `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp`, and it will break cross-origin embeds that don't send CORP/CORS headers. That's a deliberate opt-in, not something to switch on casually. Pass `mode: "shared"` to make it a hard requirement (it throws instead of degrading) or `mode: "transfer"` to opt out entirely.
- **Reading.** `read()` returns a view onto live shared memory — perfect for a renderer that immediately uploads it (`bufferSubData` straight from the view: sim in the worker, one copy total). If the data has to outlive the read, use `readInto(dst)`, which copies under the seqlock and retries if a publish lands mid-copy. `torn` counts those retries.
- **Live-view boundary.** The view `read()` returns (in either mode) is valid for exactly `count - 1` further `produce()` calls and is overwritten on the `count`-th, when the pool wraps back to the slot it points at. That is structural to a fixed pool -- do not hold the view across a yield. Use `readInto(dst)` for data that must survive past the next frame.
- **Backpressure is unchanged.** Latest-wins either way. In shared mode the writer always has a slot, so `produce()` returns `true` and an unread frame is superseded in place; `dropped` still counts frames the consumer never saw.
- `frameChannel` uses reserved `lw:fc:sab`/`lw:fc:hello` typed message types for the SAB handshake -- don't send those yourself.

**What it actually buys you, measured.** Run `npm run gate`:

```
[1] transferable ring   1.22 B retained/frame · 556K frames/s
[2] shared (SAB) mode   1.91 B retained/frame · 758K frames/s · torn=0
    shared vs transferable: 1.36x throughput, and zero postMessage traffic
```

(That multiplier moves around — repeat runs on the same machine ranged ~1.4x to ~2.1x. Treat it as "meaningfully faster", not a fixed number, and re-run the gate on your own hardware.)

Note what the retention numbers *don't* say. Both modes sit at the heap-sampling noise floor, because the ring's per-hop view header is **transient garbage, not retention** — a surviving-allocation gate can't see it, and neither mode retains anything per frame. The honest wins for shared mode are throughput and the disappearance of message traffic, not a lower retained-bytes number. If you were expecting "SAB finally makes it zero-alloc": the ring was already there on retention.

## adoptCanvas — OffscreenCanvas without the footguns (v1.2.0)

`transferControlToOffscreen()` is easy; the two things everyone forgets are **resize forwarding** and **visibility forwarding**. `adoptCanvas` does the transfer and both of those, and gives the worker an auto-pausing render loop.

```js
// main: hand the canvas to the worker and keep it in sync
const adoption = sim.adoptCanvas(document.querySelector("canvas"));
// adoption.pause() / resume() / resize() / dispose() available

// worker: receive it and draw, with resize + auto-pause handled
defineWorker((ctx) => {
  ctx.onCanvas((canvas, ctl) => {
    const g = canvas.getContext("2d");
    ctl.onResize((w, h, dpr) => { /* canvas is already resized; recompute layout */ });
    ctl.frame((dt) => { /* ...draw... */ });   // timer-driven, auto-pauses when the tab is hidden
  });
});
```

- **Resize** — a `ResizeObserver` on the main-thread element forwards size changes to the worker in device pixels (with `dpr`); `adoptCanvas` applies them to the OffscreenCanvas and calls your `onResize`.
- **Visibility** — `visibilitychange` is forwarded so `ctl.frame()` pauses when the tab is hidden and resumes when it returns. The loop prefers `requestAnimationFrame` (which modern browsers expose on the worker global alongside OffscreenCanvas) for vsync-smooth pacing, and falls back to a timer where it's absent or when you pass an explicit `{ fps }`. The forwarded visibility is what keeps the timer fallback from burning a core in a hidden tab.
- **Feature-detect** — `adoptCanvas` throws if the canvas can't be transferred; guard with `typeof canvas.transferControlToOffscreen === "function"` and fall back to main-thread drawing.
- `adoptCanvas` uses reserved `lw:canvas*` typed message types — don't send those yourself.

When the worker both produces *and* draws (as here), the frames never cross back to the main thread, so no `frameChannel` is involved — that's the point. `frameChannel` is for when the frames must reach the main thread or a second worker; `adoptCanvas` is for when the worker can own the pixels outright. The demo shows both, one per tab.

## Constraints & gotchas

- **Self-contained body.** The function is `.toString()`-serialized; it cannot capture variables from the surrounding module. Pass everything in via `post`/`send`, or inline it in the body. This matters especially under a **minifier** (Terser, esbuild, etc.): a bundler doesn't know the function will be stringified, so any outer variable it closes over gets renamed in the module but *not* inside the serialized string — the worker then throws `ReferenceError` at runtime. Keep the body free of closures and outer references. (`lite-worker`'s own serialized helpers reference only their arguments and true globals, so minifying the library itself is safe.)
- **CSP.** Blob workers need `worker-src blob:` (or `child-src blob:` on older policies) in your Content-Security-Policy.
- **Transferables by default, SharedArrayBuffer by opt-in.** The raw transport is built on transfer semantics and needs **no** cross-origin isolation; raw payloads must be `ArrayBuffer`s or ArrayBuffer views. `frameChannel` can additionally negotiate a `SharedArrayBuffer` where the page is cross-origin isolated — see [Shared mode](#shared-mode--the-sharedarraybuffer-fast-path-v130). It degrades to the ring silently, so nothing breaks when isolation is absent.
- **No Blob URL leak.** The object URL is revoked the instant the `Worker` is constructed — the browser has already fetched the script by then. Spinning up many short-lived workers without ever calling `destroy()` leaves no object URLs piling up for the document's lifetime. `destroy()` is still the right call to free the worker *thread*; it just isn't what frees the URL.
- **One extra core, not N.** A single worker is about keeping the main thread clean. For fan-out across cores, that's `lite-worker-pool` (v2.0.0).

## Roadmap

- **v1.0.0** — inline worker core: `defineWorker`, typed request/response with transferables, `spawn()` / `terminate()` / idempotent `destroy()`.
- **v1.1.0** — `frameChannel`: bounded, latest-wins frame passing over a fixed transfer pool, with lite-gl `LAYOUT`-compatible strides.
- **v1.2.0** — `adoptCanvas`: `transferControlToOffscreen()` plus resize forwarding (ResizeObserver) and visibility forwarding (auto-pause), with a worker-side `ctx.onCanvas` render loop.
- **v1.3.0** (this release) — shared mode: a `SharedArrayBuffer` double-buffer with an `Atomics` seqlock (the sim writes one slot while the consumer reads another, flipping on the frame boundary) — zero `postMessage` traffic in steady state. Feature-detected behind cross-origin isolation (COOP/COEP) and negotiated automatically; falls back transparently to the transferable ring, which stays the default.
- **v2.0.0** — `lite-worker-pool` (separate package): an N-worker pool with a `map(items, workerFn)` surface and per-worker transferable scratch buffers, for embarrassingly parallel batch work. Depends on core; core never depends on it.

See [`CHANGELOG.md`](./CHANGELOG.md) for the full release history and [`llms.txt`](./llms.txt) for a machine-readable API digest.

## License

MIT © Zahary Shinikchiev
