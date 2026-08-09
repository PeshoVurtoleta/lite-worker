// test/torture/harness.mjs
//
// Shared scaffolding for the lite-worker torture suite: loopback Blob/URL/Worker
// mocks (with live-resource counters), in-process transports that mirror the
// real raw + typed planes, a seeded xorshift32 PRNG, a settle helper, the
// lite-leak tracker wiring, and a worker_threads `self`-shim scaffold that W1's
// real-thread seqlock tier (T3/T5) will build on.
//
// All scratch that a hot loop touches is allocated by the CALLER once, outside
// the loop -- this file only provides constructors and stateless helpers.

import {
  createLeakTracker,
  createOwnerCascadeOrphanKernel,
  createAsyncRetentionKernel,
} from "@zakkster/lite-leak";

// ---------------------------------------------------------------------------
// Loopback Blob/URL/Worker mocks. Same shape as test/Worker.test.js so the
// ACTUAL serialized worker runtime runs in-process, plus counters so the
// lifecycle/soak tiers can prove nothing is retained across spawn/destroy.
// ---------------------------------------------------------------------------
const sources = new Map(); // url -> source string
let urlN = 0;
let liveWorkers = 0;

globalThis.Blob = class Blob {
  constructor(parts) { this._src = parts.join(""); }
};
globalThis.URL = {
  createObjectURL(blob) { const u = "blob:torture/" + ++urlN; sources.set(u, blob._src); return u; },
  revokeObjectURL(u) { sources.delete(u); }
};
globalThis.Worker = class Worker {
  constructor(url) {
    liveWorkers++;
    this.onmessage = null; this.onerror = null; this.onmessageerror = null;
    this._terminated = false;
    const self = {
      onmessage: null,
      close: () => { this._closed = true; },
      postMessage: (data) => {
        if (this._terminated) return;
        const snap = structuredClone(data);
        queueMicrotask(() => { if (!this._terminated && this.onmessage) this.onmessage({ data: snap }); });
      }
    };
    this._self = self;
    // eslint-disable-next-line no-new-func
    new Function("self", sources.get(url))(self);
  }
  postMessage(data) {
    if (this._terminated) return;
    const snap = structuredClone(data);
    queueMicrotask(() => { if (!this._terminated && this._self.onmessage) this._self.onmessage({ data: snap }); });
  }
  terminate() { if (!this._terminated) { this._terminated = true; liveWorkers--; } }
};

/** Live-resource introspection for the lifecycle (T2) and soak (T7) tiers. */
export const mockRegistry = {
  liveUrls() { return sources.size; },
  liveWorkers() { return liveWorkers; },
};

// ---------------------------------------------------------------------------
// Transports. Three shapes, matching the ones the test file and gate use.
// ---------------------------------------------------------------------------

// Async raw-only delivery (microtask), { send, onRaw }.
export function pair() {
  const mk = () => ({
    _h: new Set(), other: null,
    onRaw(fn) { this._h.add(fn); return () => this._h.delete(fn); },
    send(buf) { const o = this.other; queueMicrotask(() => o._h.forEach((fn) => fn(buf))); }
  });
  const a = mk(), b = mk(); a.other = b; b.other = a; return [a, b];
}

// Async raw + typed plane (for SAB handshake negotiation).
export function pairTyped() {
  const mk = () => ({
    _r: new Set(), _t: new Map(), other: null,
    onRaw(fn) { this._r.add(fn); return () => this._r.delete(fn); },
    send(buf) { const o = this.other; queueMicrotask(() => o._r.forEach((f) => f(buf))); },
    on(type, fn) { let s = this._t.get(type); if (!s) { s = new Set(); this._t.set(type, s); } s.add(fn); return () => s.delete(fn); },
    post(type, data) { const o = this.other; queueMicrotask(() => { const s = o._t.get(type); if (s) s.forEach((f) => f(data)); }); }
  });
  const a = mk(), b = mk(); a.other = b; b.other = a; return [a, b];
}

// Synchronous delivery: a "frame" is one unit of work with no event-loop noise,
// so measureOps can gate it and conservation is observable at rest. `typed`
// carries the post/on plane needed to negotiate shared mode.
export function syncPair(typed) {
  const mk = () => ({
    _r: new Set(), _t: new Map(), other: null,
    onRaw(fn) { this._r.add(fn); return () => this._r.delete(fn); },
    send(buf) { this.other._r.forEach((f) => f(buf)); },
    on(type, fn) { let s = this._t.get(type); if (!s) { s = new Set(); this._t.set(type, s); } s.add(fn); return () => s.delete(fn); },
    post(type, data) { const s = this.other._t.get(type); if (s) s.forEach((f) => f(data)); }
  });
  const a = mk(), b = mk(); a.other = b; b.other = a;
  return typed
    ? [a, b]
    : [{ onRaw: a.onRaw.bind(a), send: a.send.bind(a) }, { onRaw: b.onRaw.bind(b), send: b.send.bind(b) }];
}

// ---------------------------------------------------------------------------
// Seeded xorshift32 PRNG. Returns floats in [0, 1). Deterministic and
// allocation-free after construction so a failing op index is replayable via
// TORTURE_SEED.
// ---------------------------------------------------------------------------
export function xorshift32(seed) {
  let s = seed | 0; if (s === 0) s = 0x2545f491;
  return function () {
    s ^= s << 13; s ^= s >>> 17; s ^= s << 5;
    return (s >>> 0) / 4294967296;
  };
}

// ---------------------------------------------------------------------------
// GC entries arrive asynchronously; await a settle tick before reading any
// summary or tracker size, or the window is empty and the gate falsely passes.
// ---------------------------------------------------------------------------
export function settle(ms) {
  globalThis.gc?.();
  return new Promise((r) => setTimeout(r, ms || 50));
}

// ---------------------------------------------------------------------------
// lite-leak tracker. Kernels that do NOT patch a global surface (owner-cascade
// + async-retention) -- lite-worker's timer/global patching lives on the canvas
// loop, which the wired tiers do not exercise, so patching setTimeout here would
// only add warning noise to settle().
// ---------------------------------------------------------------------------
export function makeTracker() {
  const leaks = [];
  const warns = [];
  const tracker = createLeakTracker({
    name: "lite-worker-torture",
    onLeak: (r) => leaks.push(r.kind + ":" + String(r.tag)),
    onWarning: (w) => warns.push(w.kind + ":" + w.reason),
  });
  tracker.registerKernel(createOwnerCascadeOrphanKernel());
  tracker.registerKernel(createAsyncRetentionKernel());
  return { tracker, leaks, warns };
}

// Held-value contract: the cleanup closure passed to tracker.track MUST NOT
// close over the tracked target, or finalization is defeated. A shared no-op
// captures nothing.
export const NOOP_CLEANUP = function () {};

// ---------------------------------------------------------------------------
// worker_threads `self`-shim scaffold (for W1's real-thread T3/T5).
//
// Worker.js's serialized WORKER_RUNTIME talks to the browser worker globals
// `self.postMessage` / `self.onmessage` / `self.close` and expects real
// SharedArrayBuffer + Atomics. Inside a node:worker_threads Worker those atomics
// are already real; only the `self` bridge is missing. installSelfShim wires it
// to a parentPort so a real second thread can evaluate the serialized source
// unchanged. Not used by the tiers wired this session -- present so W1 does not
// have to build the plumbing.
// ---------------------------------------------------------------------------
export function installSelfShim(port, globalObj) {
  const g = globalObj || {};
  let handler = null;
  let wired = false;
  g.postMessage = (data, transfer) => port.postMessage(data, transfer || []);
  g.close = () => { if (typeof port.close === "function") port.close(); };
  Object.defineProperty(g, "onmessage", {
    configurable: true,
    get() { return handler; },
    set(fn) {
      handler = fn;
      if (!wired) {
        wired = true;
        port.on("message", (data) => { if (handler) handler({ data }); });
      }
    }
  });
  return g;
}
