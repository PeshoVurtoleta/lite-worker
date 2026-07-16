/**
 * @zakkster/lite-worker — type declarations.
 * @copyright Zahary Shinikchiev
 */

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
  /** Pool size — the number of buffers that ping-pong. Default 2, minimum 2. */
  count?: number;
  /** Instance capacity, required when `layout` is a bare stride number. */
  capacity?: number;
  /** Consumer-only: called with the view each time a new frame swaps in. */
  onFrame?: (view: Float32Array | Uint8Array, buffer: ArrayBuffer) => void;
}

interface FrameChannelBase {
  readonly role: "producer" | "consumer";
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
   * Fill the next free buffer via `fill(view, buffer)` and transfer it to the
   * consumer. Returns `false` when no buffer is free — the frame is dropped
   * (latest-wins) and your loop should simply advance to the next tick. The
   * channel never queues unboundedly.
   */
  produce(fill: (view: Float32Array | Uint8Array, buffer: ArrayBuffer) => void): boolean;
  /** Buffers currently free to write. */
  readonly free: number;
}

/** Consumer half: reads the freshest frame. */
export interface ConsumerFrameChannel extends FrameChannelBase {
  readonly role: "consumer";
  /**
   * The freshest frame's view, or `null` before the first frame arrives.
   * Non-destructive and allocation-free — the view is cached until the next frame
   * swaps in — so it is safe to call every animation frame.
   */
  read(): Float32Array | Uint8Array | null;
  /** True when a frame has arrived that has not yet been read. */
  readonly hasNew: boolean;
  /** Count of frames superseded before they were read (dropped, latest-wins). */
  readonly dropped: number;
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
