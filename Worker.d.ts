/**
 * @zakkster/lite-worker — type declarations.
 * @copyright Zahary Shinikchiev
 */

/** Package version. Kept in three-place sync with package.json and CHANGELOG.md. */
export declare const VERSION: string;

/** Options passed to {@link defineWorker}. */
export interface WorkerOptions {
  /**
   * Worker classification. `"classic"` (default) has the broadest support and
   * is correct for self-contained bodies. `"module"` enables ESM syntax inside
   * the worker but requires a browser that supports module workers from Blob URLs.
   */
  type?: "classic" | "module";
  /** Optional worker name, surfaced in devtools. */
  name?: string;
  /** Called for uncaught worker errors (also emitted as the `"error"` event). */
  onError?: (error: Error) => void;
}

/** Options for a single {@link WorkerHandle.call} request. */
export interface CallOptions {
  /** Transferables to move (not copy) with the request. */
  transfer?: Transferable[];
  /** Reject the returned promise if no reply arrives within N milliseconds. */
  timeout?: number;
}

/** A reply function handed to worker-side handlers of `call` requests. */
export type Reply<R = unknown> = (data: R, transfer?: Transferable[]) => void;

/**
 * Worker-side channel, passed as the single argument to the module function.
 * Symmetric with the main-side {@link WorkerHandle}.
 */
export interface WorkerContext {
  /**
   * Subscribe to a typed message from the main thread. For `call` requests the
   * second argument is a reply function; for `post` messages it is `undefined`.
   * Returning a non-`undefined` value (or a resolving promise) from a `call`
   * handler auto-replies when `reply` was not called explicitly.
   */
  on<D = unknown, R = unknown>(
    type: string,
    handler: (data: D, reply?: Reply<R>) => void | R | Promise<R>
  ): () => void;
  /** Remove a typed handler previously registered with {@link WorkerContext.on}. */
  off(type: string, handler: (...args: any[]) => any): void;

  /** Subscribe to raw transferred buffers from the main thread. */
  onRaw(handler: (buffer: ArrayBuffer | ArrayBufferView) => void): () => void;
  /** Remove a raw handler. */
  offRaw(handler: (buffer: ArrayBuffer | ArrayBufferView) => void): void;

  /** Fire-and-forget typed message to the main thread. */
  post(type: string, data?: unknown, transfer?: Transferable[]): void;
  /**
   * Raw transferred send to the main thread. If `transfer` is omitted and the
   * payload is an ArrayBuffer/view, its buffer is auto-transferred (detached
   * on this side). This is the zero-GC hot path.
   */
  send(buffer: ArrayBuffer | ArrayBufferView, transfer?: Transferable[]): void;

  /**
   * Build a bounded, latest-wins frame channel over this worker's raw transport.
   * See {@link frameChannel}. One frame channel owns the raw stream — do not also
   * use {@link WorkerContext.send}/{@link WorkerContext.onRaw} directly while it
   * is live.
   */
  frameChannel(
    layout: FrameLayout,
    options: FrameChannelOptions & { role: "producer" }
  ): ProducerFrameChannel;
  frameChannel(
    layout: FrameLayout,
    options: FrameChannelOptions & { role: "consumer" }
  ): ConsumerFrameChannel;

  /**
   * Receive a canvas adopted from the main thread via
   * {@link WorkerHandle.adoptCanvas}. The callback runs once, with the transferred
   * OffscreenCanvas and a {@link CanvasControl} that surfaces forwarded resize and
   * visibility and offers an auto-pausing render loop.
   */
  onCanvas(
    cb: (canvas: OffscreenCanvas, control: CanvasControl) => void,
    options?: CanvasFrameOptions
  ): CanvasControl;

  /** Terminate this worker from the inside (`self.close()`). */
  close(): void;
}

/** Main-thread handle returned by {@link defineWorker}. */
export declare class WorkerHandle {
  private constructor();

  /** True once {@link WorkerHandle.spawn} has created the underlying Worker. */
  readonly spawned: boolean;
  /** True once {@link WorkerHandle.destroy} has run; the handle is unusable. */
  readonly destroyed: boolean;

  /**
   * Create the underlying Worker. Idempotent while spawned (returns `this`).
   * Throws if the handle was destroyed.
   */
  spawn(): this;

  /**
   * Raw transferred send (zero-GC hot path). No-op before spawn / after
   * terminate. If `transfer` is omitted and the payload is an ArrayBuffer/view,
   * its buffer is auto-transferred and becomes detached on this side.
   */
  send(buffer: ArrayBuffer | ArrayBufferView, transfer?: Transferable[]): void;
  /** Subscribe to raw buffers pushed from the worker. Returns an unsubscribe fn. */
  onRaw(handler: (buffer: ArrayBuffer | ArrayBufferView) => void): () => void;
  /** Remove a raw handler. */
  offRaw(handler: (buffer: ArrayBuffer | ArrayBufferView) => void): void;

  /** Fire-and-forget typed message. No-op before spawn / after terminate. */
  post(type: string, data?: unknown, transfer?: Transferable[]): void;
  /**
   * Typed request/response. Resolves with the worker's reply, or rejects if the
   * worker handler throws, there is no handler, the call times out, or the
   * worker is terminated. Allocates per call — use {@link WorkerHandle.send}
   * for per-frame traffic.
   */
  call<R = unknown>(type: string, data?: unknown, opts?: CallOptions): Promise<R>;
  /** Subscribe to a typed message pushed from the worker. Returns unsubscribe fn. */
  on<D = unknown>(type: string, handler: (data: D) => void): () => void;
  /** Remove a typed handler. */
  off(type: string, handler: (...args: any[]) => any): void;

  /**
   * Build a bounded, latest-wins frame channel over this handle's raw transport.
   * See {@link frameChannel}. One frame channel owns the raw stream — do not also
   * use {@link WorkerHandle.send}/{@link WorkerHandle.onRaw} directly while it is
   * live. For a producer channel here, call {@link WorkerHandle.spawn} first.
   */
  frameChannel(
    layout: FrameLayout,
    options: FrameChannelOptions & { role: "producer" }
  ): ProducerFrameChannel;
  frameChannel(
    layout: FrameLayout,
    options: FrameChannelOptions & { role: "consumer" }
  ): ConsumerFrameChannel;

  /**
   * Hand a canvas to the worker via `transferControlToOffscreen()` and keep it in
   * sync: a ResizeObserver forwards size changes (device pixels + dpr) and
   * `visibilitychange` forwards tab visibility so the worker's render loop can
   * auto-pause. Pair with {@link WorkerContext.onCanvas}. Throws if the canvas does
   * not support `transferControlToOffscreen`.
   */
  adoptCanvas(canvasEl: HTMLCanvasElement, options?: AdoptCanvasOptions): CanvasAdoption;

  /**
   * Terminate the worker and reject all pending calls. The handle stays
   * reusable — call {@link WorkerHandle.spawn} again to restart.
   */
  terminate(): this;
  /**
   * Idempotent full teardown: terminate, drop all handlers, and mark the handle
   * dead. Safe to call multiple times. After this, spawn() throws.
   */
  destroy(): void;
}

/** The raw-transport shape frameChannel needs. Both {@link WorkerHandle} and {@link WorkerContext} satisfy it. */
export interface RawTransport {
  send(buffer: ArrayBuffer | ArrayBufferView, transfer?: Transferable[]): void;
  onRaw(handler: (buffer: ArrayBuffer | ArrayBufferView) => void): () => void;
}

/**
 * Frame buffer layout. A number is a Float32 stride (floats per instance, e.g.
 * lite-gl `LAYOUT.POINT`) and needs `capacity` in the options; `{ stride,
 * capacity }` is the same but self-contained; `{ bytes }` is a generic byte buffer.
 */
export type FrameLayout =
  | number
  | { stride: number; capacity: number }
  | { bytes: number };

/** Options for {@link frameChannel}. */
export interface FrameChannelOptions {
  /** Which half this endpoint is. Required. */
  role: "producer" | "consumer";
  /**
   * Transport for the frames.
   *  - `"auto"` (default) — use the SharedArrayBuffer fast path when it's
   *    available (cross-origin isolated, and the transport has `post`/`on`),
   *    otherwise fall back to the transferable ring transparently.
   *  - `"shared"` (alias `"sab"`) — require the shared path; throws when it
   *    isn't available rather than silently degrading.
   *  - `"transfer"` — always use the transferable ring.
   */
  mode?: "auto" | "shared" | "sab" | "transfer";
  /** Pool size — the number of buffers that ping-pong (or shared slots). Default 2, minimum 2. */
  count?: number;
  /** Instance capacity, required when `layout` is a bare stride number. */
  capacity?: number;
  /** Consumer-only, transferable mode: called with the view each time a new frame swaps in. */
  onFrame?: (view: Float32Array | Uint8Array, buffer: ArrayBuffer) => void;
}

interface FrameChannelBase {
  readonly role: "producer" | "consumer";
  /** Which transport this endpoint settled on. */
  readonly mode: "shared" | "transfer";
  /** True when running on the SharedArrayBuffer fast path. */
  readonly shared: boolean;
  /** `"f32"` for stride layouts, `"bytes"` for `{ bytes }` layouts. */
  readonly kind: "f32" | "bytes";
  /** Float stride (0 for a `{ bytes }` layout). */
  readonly stride: number;
  /** Instance capacity (0 for a `{ bytes }` layout). */
  readonly capacity: number;
  /** Byte length of each pooled buffer. */
  readonly byteLength: number;
  /** Number of pooled buffers. */
  readonly count: number;
  /** Detach from the transport and release references. Idempotent. */
  dispose(): void;
}

/** Producer half: fills and publishes frames. */
export interface ProducerFrameChannel extends FrameChannelBase {
  readonly role: "producer";
  /**
   * Fill the next free buffer via `fill(view, buffer)` and publish it. In
   * transferable mode this transfers the buffer and returns `false` when no
   * buffer is free — the frame is dropped (latest-wins) and your loop advances.
   * In shared mode the writer always has a slot, so it returns `true` and an
   * unread frame is simply superseded in place. Either way nothing queues.
   */
  produce(fill: (view: Float32Array | Uint8Array, buffer: ArrayBuffer | SharedArrayBuffer) => void): boolean;
  /** Buffers currently free to write (always `count` in shared mode). */
  readonly free: number;
  /** Shared mode only: total frames published. */
  readonly published?: number;
}

/** Consumer half: reads the freshest frame. */
export interface ConsumerFrameChannel extends FrameChannelBase {
  readonly role: "consumer";
  /**
   * The freshest frame's view, or `null` before the first frame arrives.
   * Non-destructive and allocation-free — views are cached (one per slot in
   * shared mode, one per buffer in transferable mode) — so it is safe to call
   * every animation frame. In shared mode the returned view aliases live shared
   * memory: consume it immediately, or use {@link ConsumerFrameChannel.readInto}
   * for a snapshot that cannot change underneath you.
   *
   * Live-view boundary (both modes): the returned view stays valid for exactly
   * `count - 1` further `produce()` calls and is overwritten on the `count`-th,
   * when the pool wraps back to the slot it points at. This is structural to a
   * fixed pool -- do not hold the view across a yield; use
   * {@link ConsumerFrameChannel.readInto} for data that must outlive the read.
   */
  read(): Float32Array | Uint8Array | null;
  /**
   * Copy the freshest frame into `dst`, re-checking the seqlock so a publish
   * that lands mid-copy is retried. Returns `false` if nothing has been
   * published yet or the retries were exhausted. Use when the data outlives the
   * read; {@link ConsumerFrameChannel.read} is the fast path.
   */
  readInto(dst: Float32Array | Uint8Array): boolean;
  /** True when a frame has arrived that has not yet been read. */
  readonly hasNew: boolean;
  /** Count of frames superseded before they were read (dropped, latest-wins). */
  readonly dropped: number;
  /** Shared mode only: reads that raced an in-flight publish and retried. */
  readonly torn: number;
}

export type FrameChannel = ProducerFrameChannel | ConsumerFrameChannel;

/**
 * Build a bounded, latest-wins frame channel over a raw transport (a
 * {@link WorkerHandle} or a worker-side {@link WorkerContext}).
 *
 * A fixed pool of `count` ArrayBuffers (default 2) ping-pongs between producer
 * and consumer via ownership transfer: no per-frame data allocation, no object
 * clone, and memory that cannot grow. When the consumer falls behind, intermediate
 * frames drop (latest-wins) instead of queueing.
 */
export declare function frameChannel(
  transport: RawTransport,
  layout: FrameLayout,
  options: FrameChannelOptions & { role: "producer" }
): ProducerFrameChannel;
export declare function frameChannel(
  transport: RawTransport,
  layout: FrameLayout,
  options: FrameChannelOptions & { role: "consumer" }
): ConsumerFrameChannel;

/** Options for {@link WorkerHandle.adoptCanvas}. */
export interface AdoptCanvasOptions {
  /** Clamp devicePixelRatio when sizing the transferred bitmap. Default 2. */
  maxDpr?: number;
}

/** Main-side controller returned by {@link WorkerHandle.adoptCanvas}. */
export interface CanvasAdoption {
  /** Re-measure the canvas and forward its size to the worker. */
  resize(): void;
  /** Force the worker's render loop to pause. */
  pause(): void;
  /** Force the worker's render loop to resume. */
  resume(): void;
  /** Disconnect the ResizeObserver and remove the resize/visibility listeners. */
  dispose(): void;
}

/** Options for a worker-side render loop ({@link CanvasControl.frame}, {@link WorkerContext.onCanvas}). */
export interface CanvasFrameOptions {
  /** Target frames per second. Overrides `interval`. */
  fps?: number;
  /** Millisecond interval between frames. Default ~16.7 (60fps). */
  interval?: number;
}

/** Worker-side control handed to the {@link WorkerContext.onCanvas} callback. */
export interface CanvasControl {
  /** The transferred OffscreenCanvas. */
  readonly canvas: OffscreenCanvas;
  /** Current bitmap width in device pixels. */
  readonly width: number;
  /** Current bitmap height in device pixels. */
  readonly height: number;
  /** Current devicePixelRatio forwarded from the main thread. */
  readonly dpr: number;
  /** Whether the owning tab is currently visible. */
  readonly visible: boolean;
  /** Register a resize listener, fired after `canvas.width`/`height` is applied. Returns an off function. */
  onResize(fn: (width: number, height: number, dpr: number) => void): () => void;
  /** Register a visibility listener (true when the tab is visible). Returns an off function. */
  onVisibility(fn: (visible: boolean) => void): () => void;
  /**
   * Start a render loop that auto-pauses when the owning tab is hidden. Prefers
   * `requestAnimationFrame` (exposed on the worker global alongside OffscreenCanvas)
   * for vsync pacing, falling back to a timer where it's absent or when `fps`/
   * `interval` is given. The callback receives the delta time in ms. Returns a
   * stop function.
   */
  frame(fn: (dt: number) => void, options?: CanvasFrameOptions): () => void;
  /** Manually pause the render loop. */
  pause(): void;
  /** Manually resume the render loop. */
  resume(): void;
  /** Stop the loop and remove all listeners. */
  dispose(): void;
}

/**
 * Create a worker from a self-contained module function. The function is
 * serialized into a Blob URL — no separate file, no bundler configuration. It
 * cannot reference anything from the enclosing scope and receives its channel
 * as the single `ctx` argument. The worker is created lazily on spawn().
 */
export declare function defineWorker(
  moduleFn: (ctx: WorkerContext) => void,
  options?: WorkerOptions
): WorkerHandle;

export default defineWorker;
