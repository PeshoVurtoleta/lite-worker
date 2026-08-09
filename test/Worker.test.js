// Loopback test harness. Mocks Blob/URL/Worker so the *actual* serialized
// worker runtime (and the user module fn) runs in-process, wired to the
// main-side handle through microtask-queued postMessage in both directions.
// This exercises real code on both sides -- envelope discrimination, reply
// correlation, raw transport, lifecycle -- without a browser.
//
// Runner: node:test (node --test). Assertions via node:assert/strict.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// Capture the native URL before the loopback mock below shadows it, so the
// version-sync test can still resolve package.json.
const NativeURL = globalThis.URL;

const sources = new Map(); // url -> source string
let urlN = 0;

globalThis.Blob = class Blob {
  constructor(parts) { this._src = parts.join(""); }
};
globalThis.URL = {
  createObjectURL(blob) { const u = "blob:mock/" + ++urlN; sources.set(u, blob._src); return u; },
  revokeObjectURL(u) { sources.delete(u); }
};
globalThis.Worker = class Worker {
  constructor(url) {
    this.onmessage = null; this.onerror = null; this.onmessageerror = null;
    this._terminated = false;
    const self = {
      onmessage: null,
      close: () => { this._closed = true; },
      postMessage: (data) => {
        if (this._terminated) return;
        const snap = structuredClone(data); // real postMessage clones synchronously at call time
        queueMicrotask(() => { if (!this._terminated && this.onmessage) this.onmessage({ data: snap }); });
      }
    };
    this._self = self;
    // Run the serialized worker source with `self` bound to our mock.
    // eslint-disable-next-line no-new-func
    new Function("self", sources.get(url))(self);
  }
  postMessage(data) {
    if (this._terminated) return;
    const snap = structuredClone(data); // real postMessage clones synchronously at call time
    queueMicrotask(() => { if (!this._terminated && this._self.onmessage) this._self.onmessage({ data: snap }); });
  }
  terminate() { this._terminated = true; }
};

const { defineWorker, frameChannel, VERSION } = await import("../Worker.js");

async function throws(fn, match) {
  try { await fn(); return false; }
  catch (e) { return match ? String(e.message).includes(match) : true; }
}
function once(h, type) { return new Promise((res) => { const off = h.on(type, (d) => { off(); res(d); }); }); }
function onceRaw(h) { return new Promise((res) => { const off = h.onRaw((b) => { off(); res(b); }); }); }
const tick = () => new Promise((r) => setTimeout(r, 0));

// A linked in-process transport pair: models async raw delivery (microtask)
// between two endpoints, each with the { send, onRaw } shape frameChannel needs.
function pair() {
  const mk = () => ({
    _h: new Set(), other: null,
    onRaw(fn) { this._h.add(fn); return () => this._h.delete(fn); },
    send(buf) { const o = this.other; queueMicrotask(() => o._h.forEach((fn) => fn(buf))); }
  });
  const a = mk(), b = mk(); a.other = b; b.other = a; return [a, b];
}

// Like pair(), but also carrying the typed plane (post/on) that the SAB
// handshake needs to negotiate shared mode.
function pairTyped() {
  const mk = () => ({
    _r: new Set(), _t: new Map(), other: null,
    onRaw(fn) { this._r.add(fn); return () => this._r.delete(fn); },
    send(buf) { const o = this.other; queueMicrotask(() => o._r.forEach((f) => f(buf))); },
    on(type, fn) { let s = this._t.get(type); if (!s) { s = new Set(); this._t.set(type, s); } s.add(fn); return () => s.delete(fn); },
    post(type, data) { const o = this.other; queueMicrotask(() => { const s = o._t.get(type); if (s) s.forEach((f) => f(data)); }); }
  });
  const a = mk(), b = mk(); a.other = b; b.other = a; return [a, b];
}

const SAB_OK = typeof SharedArrayBuffer !== "undefined" && typeof Atomics !== "undefined";

// --- 0. version sync -------------------------------------------------------
test("0. VERSION matches package.json version (three-place sync)", () => {
  const pkg = JSON.parse(readFileSync(new NativeURL("../package.json", import.meta.url), "utf8"));
  assert.equal(VERSION, pkg.version, "VERSION === package.json version");
});

// --- 1. lifecycle basics ---------------------------------------------------
test("1. lifecycle basics", () => {
  const h = defineWorker((ctx) => { ctx.on("noop", () => {}); });
  assert.ok(h.spawned === false, "not spawned before spawn()");
  const same = h.spawn();
  assert.ok(same === h, "spawn() returns handle");
  assert.ok(h.spawned === true, "spawned after spawn()");
  assert.ok(h.spawn() === h && h.spawned === true, "spawn() idempotent");
  h.destroy();
});

// --- 2. post -> worker -> push back ----------------------------------------
test("2. post round-trips through worker push", async () => {
  const h = defineWorker((ctx) => { ctx.on("ping", (n) => ctx.post("pong", n + 1)); }).spawn();
  const p = once(h, "pong");
  h.post("ping", 41);
  assert.ok((await p) === 42, "post round-trips through worker push");
  h.destroy();
});

// --- 3. call: explicit reply -----------------------------------------------
test("3. call: explicit reply", async () => {
  const h = defineWorker((ctx) => { ctx.on("add", (d, reply) => reply(d.a + d.b)); }).spawn();
  assert.ok((await h.call("add", { a: 2, b: 3 })) === 5, "call() resolves with explicit reply");
  h.destroy();
});

// --- 4. call: sync return auto-reply ---------------------------------------
test("4. call: sync return auto-reply", async () => {
  const h = defineWorker((ctx) => { ctx.on("mul", (d) => d.a * d.b); }).spawn();
  assert.ok((await h.call("mul", { a: 4, b: 5 })) === 20, "call() auto-replies from sync return");
  h.destroy();
});

// --- 5. call: async return auto-reply --------------------------------------
test("5. call: async return auto-reply", async () => {
  const h = defineWorker((ctx) => { ctx.on("slow", async (d) => d * 2); }).spawn();
  assert.ok((await h.call("slow", 21)) === 42, "call() auto-replies from async return");
  h.destroy();
});

// --- 6. call: handler throws -> reject --------------------------------------
test("6. call: handler throws rejects", async () => {
  const h = defineWorker((ctx) => { ctx.on("boom", () => { throw new Error("kaboom"); }); }).spawn();
  assert.ok(await throws(() => h.call("boom"), "kaboom"), "call() rejects when handler throws");
  h.destroy();
});

// --- 7. call: no handler -> reject ------------------------------------------
test("7. call: no handler rejects", async () => {
  const h = defineWorker((ctx) => { ctx.on("known", () => 1); }).spawn();
  assert.ok(await throws(() => h.call("unknown"), "no handler"), "call() rejects with no handler");
  h.destroy();
});

// --- 8. call: timeout -------------------------------------------------------
test("8. call: timeout", async () => {
  const h = defineWorker((ctx) => { ctx.on("hang", () => undefined); }).spawn();
  assert.ok(await throws(() => h.call("hang", null, { timeout: 40 }), "timed out"), "call() times out");
  h.destroy();
});

// --- 9. raw transport both directions --------------------------------------
test("9. raw transport both directions", async () => {
  const h = defineWorker((ctx) => {
    ctx.onRaw((buf) => { const v = new Uint8Array(buf.buffer || buf); v[0] = v[0] + 1; ctx.send(v); });
  }).spawn();
  const p = onceRaw(h);
  h.send(new Uint8Array([7]));
  const got = await p;
  assert.ok(new Uint8Array(got.buffer || got)[0] === 8, "raw send()/onRaw() round-trips");
  h.destroy();
});

// --- 10. unsubscribe --------------------------------------------------------
test("10. unsubscribe stops delivery", async () => {
  const h = defineWorker((ctx) => { ctx.on("e", () => ctx.post("f", 1)); }).spawn();
  let hits = 0;
  const off = h.on("f", () => { hits++; });
  h.post("e"); await tick();
  off();
  h.post("e"); await tick();
  assert.ok(hits === 1, "on() unsubscribe stops delivery");
  h.destroy();
});

// --- 11. terminate rejects pending, handle reusable ------------------------
test("11. terminate rejects pending, handle reusable", async () => {
  const h = defineWorker((ctx) => { ctx.on("hang", () => undefined); }).spawn();
  const p = h.call("hang");
  const rejected = throws(() => p, "terminated");
  h.terminate();
  assert.ok(await rejected, "terminate() rejects pending calls");
  assert.ok(h.spawned === false && h.destroyed === false, "terminate() leaves handle re-spawnable");
  h.spawn();
  assert.ok(h.spawned === true, "re-spawn() works after terminate()");
  h.destroy();
});

// --- 12. destroy idempotent + guards ---------------------------------------
test("12. destroy idempotent + guards", async () => {
  const h = defineWorker((ctx) => { ctx.on("x", () => {}); }).spawn();
  h.destroy();
  let threw = false;
  try { h.destroy(); } catch { threw = true; }
  assert.ok(threw === false && h.destroyed === true, "destroy() is idempotent");
  assert.ok(await throws(() => { h.spawn(); }, "destroyed"), "spawn() after destroy throws");
  assert.ok((() => { h.post("x", 1); return true; })(), "post() after destroy is a no-op");
  assert.ok((() => { h.send(new Uint8Array([1])); return true; })(), "send() after destroy is a no-op");
  assert.ok(await throws(() => h.call("x"), "destroyed"), "call() after destroy rejects");
});

// --- 13. defineWorker guards ------------------------------------------------
test("13. defineWorker guards", () => {
  let threw = false;
  try { defineWorker(123); } catch (e) { threw = e instanceof TypeError; }
  assert.ok(threw, "defineWorker(non-fn) throws TypeError");
});

// --- 14. frameChannel: validation ------------------------------------------
test("14. frameChannel: validation", async () => {
  const [t] = pair();
  assert.ok(await throws(() => frameChannel(t, 8, { role: "x", capacity: 4 }), "role"), "frameChannel bad role throws");
  assert.ok(await throws(() => frameChannel(t, 8, { role: "producer" }), "capacity"), "frameChannel bare stride needs capacity");
  assert.ok(await throws(() => frameChannel(t, 0, { role: "producer", capacity: 4 }), "stride"), "frameChannel zero stride throws");
  assert.ok(await throws(() => frameChannel(t, "nope", { role: "producer" })), "frameChannel garbage layout throws");
});

// --- 15. frameChannel: lite-gl LAYOUT interop shape ------------------------
test("15. frameChannel: lite-gl LAYOUT interop shape", () => {
  const [a, b] = pair();
  const cap = 100, stride = 8; // lite-gl LAYOUT.POINT
  const p = frameChannel(a, stride, { role: "producer", capacity: cap });
  const c = frameChannel(b, { stride, capacity: cap }, { role: "consumer" });
  assert.ok(p.byteLength === cap * stride * 4, "byteLength = capacity*stride*4");
  assert.ok(p.stride === 8 && p.capacity === cap && p.kind === "f32", "stride/capacity/kind exposed");
  assert.ok(p.count === 2 && c.count === 2, "count defaults to 2");
  assert.ok(frameChannel(a, { bytes: 64 }, { role: "producer" }).kind === "bytes", "{bytes} layout -> byte kind");
  p.dispose(); c.dispose();
});

// --- 16. frameChannel: data round-trip (in-process) ------------------------
test("16. frameChannel: data round-trip (in-process)", async () => {
  const [a, b] = pair();
  const stride = 4, cap = 3;
  const p = frameChannel(a, stride, { role: "producer", capacity: cap });
  const c = frameChannel(b, stride, { role: "consumer", capacity: cap });
  assert.ok(c.read() === null, "read() is null before first frame");
  p.produce((f) => { f[0] = 7; f[stride] = 9; });
  await tick();
  const v = c.read();
  assert.ok(!!v && v[0] === 7 && v[stride] === 9, "consumer reads produced values");
  assert.ok(c.read() === v, "read() view is cached (same object across reads)");
  p.dispose(); c.dispose();
});

// --- 17. frameChannel: worker-side ctx.frameChannel (serialized copy) ------
test("17. frameChannel: worker-side ctx.frameChannel (serialized copy)", async () => {
  const h = defineWorker((ctx) => {
    // self-contained: constants inlined, nothing closed over
    const ch = ctx.frameChannel(4, { role: "producer", capacity: 2 });
    let seq = 0;
    ctx.on("tick", () => { ch.produce((f) => { f[0] = ++seq; }); });
  }).spawn();
  const c = h.frameChannel({ stride: 4, capacity: 2 }, { role: "consumer" });
  h.post("tick"); await tick(); await tick();
  const v = c.read();
  assert.ok(!!v && v[0] === 1, "worker ctx.frameChannel produces to main consumer");
  h.post("tick"); h.post("tick"); await tick(); await tick();
  const v2 = c.read();
  assert.ok(!!v2 && v2[0] >= 2, "consumer advances to latest worker frame");
  h.destroy();
});

// --- 18. frameChannel: latest-wins drop ------------------------------------
test("18. frameChannel: latest-wins drop", async () => {
  const [a, b] = pair();
  const p = frameChannel(a, 4, { role: "producer", capacity: 2, count: 2 });
  const c = frameChannel(b, 4, { role: "consumer", capacity: 2, count: 2 });
  p.produce((f) => { f[0] = 1; }); await tick(); // arrives, left unread
  p.produce((f) => { f[0] = 2; }); await tick(); // supersedes the unread frame -> drop
  assert.ok(c.dropped >= 1, "dropped increments when a frame is superseded unread");
  assert.ok(c.read()[0] === 2, "consumer holds the latest, not the dropped frame");
  p.dispose(); c.dispose();
});

// --- 19. frameChannel: bounded -- pool conserved, never queues --------------
test("19. frameChannel: bounded -- pool conserved, never queues", async () => {
  const [a, b] = pair();
  const count = 3;
  const p = frameChannel(a, 2, { role: "producer", capacity: 8, count });
  const c = frameChannel(b, 2, { role: "consumer", capacity: 8, count });
  let maxFree = 0, produced = 0, dropped = 0;
  for (let round = 0; round < 3000; round++) {
    if (p.produce((f) => { f[0] = round; })) produced++; else dropped++;
    if (p.free > maxFree) maxFree = p.free;
    if ((round & 15) === 0) { await tick(); c.read(); } // deliberately slow consumer
  }
  await tick(); await tick();
  assert.ok(maxFree <= count, "producer.free never exceeds the pool count");
  assert.ok(produced + dropped === 3000, "every round is accounted (produced+dropped)");
  assert.ok(dropped > 0, "frames dropped under flood (bounded, not queued)");
  assert.ok(c.read() !== null, "consumer still has a valid latest frame after soak");
  p.dispose(); c.dispose();
});

// --- 20. frameChannel: dispose detaches ------------------------------------
test("20. frameChannel: dispose detaches", () => {
  const [a, b] = pair();
  const p = frameChannel(a, 2, { role: "producer", capacity: 4 });
  const c = frameChannel(b, 2, { role: "consumer", capacity: 4 });
  p.dispose(); c.dispose();
  assert.ok(p.produce(() => {}) === false, "produce() after dispose returns false");
  assert.ok((() => { p.dispose(); c.dispose(); return true; })(), "dispose() is idempotent");
});

// --- 21. adoptCanvas + ctx.onCanvas (protocol over the loopback) -----------
test("21. adoptCanvas + ctx.onCanvas (protocol over the loopback)", async () => {
  // minimal DOM stubs for the main side
  globalThis.window = globalThis.window || { devicePixelRatio: 2, addEventListener() {}, removeEventListener() {} };
  globalThis.document = globalThis.document || { hidden: false, addEventListener() {}, removeEventListener() {} };
  globalThis.ResizeObserver = globalThis.ResizeObserver || class { constructor(fn) { this.fn = fn; } observe() {} disconnect() { this.disconnected = true; } };

  const h = defineWorker((ctx) => {
    ctx.onCanvas((canvas, ctl) => {
      ctx.post("adopted", { w: canvas.width, h: canvas.height, dpr: ctl.dpr, hasFrame: typeof ctl.frame === "function", visible: ctl.visible });
      ctl.onResize((cw, ch, dpr) => ctx.post("resized", { cw, ch, dpr }));
      ctl.onVisibility((v) => ctx.post("vis", { v }));
      ctl.frame(() => ctx.post("drew", {}), { fps: 120 });
    });
  }).spawn();

  const got = {}; let drew = 0;
  h.on("adopted", (d) => (got.adopted = d));
  h.on("resized", (d) => (got.resized = d));
  h.on("vis", (d) => (got.vis = d));
  h.on("drew", () => drew++);

  assert.ok(await throws(() => h.adoptCanvas({}), "transferControlToOffscreen"),
    "adoptCanvas throws without transferControlToOffscreen");

  const fakeCanvas = {
    width: 0, height: 0,
    getBoundingClientRect: () => ({ width: 300, height: 150 }),
    transferControlToOffscreen: () => ({ width: 0, height: 0 })
  };
  const adoption = h.adoptCanvas(fakeCanvas);
  await tick();
  assert.ok(got.adopted && got.adopted.w === 600 && got.adopted.h === 300, "onCanvas receives device dims (dpr applied)");
  assert.ok(got.adopted && got.adopted.dpr === 2 && got.adopted.hasFrame && got.adopted.visible === true, "ctl exposes dpr + frame + visible");

  adoption.resize(); await tick();
  assert.ok(got.resized && got.resized.cw === 600 && got.resized.dpr === 2, "resize is forwarded to the worker");

  adoption.pause(); await tick();
  assert.ok(got.vis && got.vis.v === false, "pause forwards visibility=false");
  adoption.resume(); await tick();
  assert.ok(got.vis && got.vis.v === true, "resume forwards visibility=true");

  await new Promise((r) => setTimeout(r, 50)); // let the ~120fps worker timer fire
  assert.ok(drew > 0, "frame() render loop fires (timer-driven, auto-paused when hidden)");

  // Pause the worker-side loop before teardown so it stops rescheduling its
  // in-process setTimeout; otherwise node:test's event loop never drains.
  adoption.pause();
  await new Promise((r) => setTimeout(r, 20));
  adoption.dispose();
  assert.ok(true, "adoption.dispose() does not throw");
  h.destroy();
});

// --- 22. spawn/destroy leak gate: no orphaned Blob URLs --------------------
test("22. spawn/destroy leak gate: no orphaned Blob URLs", () => {
  const before = sources.size;
  for (let i = 0; i < 50; i++) {
    const h = defineWorker((ctx) => { ctx.on("noop", () => {}); });
    h.spawn();
    h.terminate();
    h.destroy();
  }
  assert.ok(sources.size === before, "no Blob URLs retained across 50 spawn/destroy cycles");
  const h = defineWorker(() => {}).spawn();
  assert.ok(sources.size === before, "spawned worker holds no lingering object URL (revoked on construction)");
  h.destroy();
  assert.ok(h.destroyed === true, "destroy() marks the handle destroyed");
});

// --- 23. worker error rejects in-flight calls (no hang to timeout) ---------
test("23. worker error rejects in-flight calls (no hang to timeout)", async () => {
  const h = defineWorker((ctx) => {
    ctx.on("hang", () => new Promise(() => {})); // never resolves
  }).spawn();
  const p = h.call("hang", null, { timeout: 60000 }); // long timeout; must NOT wait for it
  await tick();
  // simulate a worker load/parse/uncaught failure
  h._worker.onerror({ message: "simulated worker failure" });
  let rejected = false, msg = "";
  try { await p; } catch (e) { rejected = true; msg = e.message; }
  assert.ok(rejected && /simulated worker failure/.test(msg), "worker error rejects the pending call immediately");
  assert.ok(h._pending.size === 0, "pending map cleared after error");
  h.destroy();
});

// --- 24. render loop prefers requestAnimationFrame when available ----------
test("24. render loop prefers requestAnimationFrame when available", async () => {
  let rafCalls = 0;
  const savedRaf = globalThis.requestAnimationFrame;
  const savedCaf = globalThis.cancelAnimationFrame;
  globalThis.requestAnimationFrame = (fn) => { rafCalls++; return setTimeout(() => fn(performance.now()), 8); };
  globalThis.cancelAnimationFrame = (id) => clearTimeout(id);
  try {
    const h = defineWorker((ctx) => {
      ctx.onCanvas((canvas, ctl) => {
        ctl.frame(() => ctx.post("rf", {})); // no fps -> should take the rAF path
      });
    }).spawn();
    let drew = 0;
    h.on("rf", () => drew++);
    const fake = { width: 0, height: 0, getBoundingClientRect: () => ({ width: 10, height: 10 }), transferControlToOffscreen: () => ({ width: 0, height: 0 }) };
    const adoption = h.adoptCanvas(fake);
    await new Promise((r) => setTimeout(r, 60));
    assert.ok(rafCalls > 0, "frame() without an explicit rate uses requestAnimationFrame");
    assert.ok(drew > 0, "rAF-driven loop actually renders");
    // Pause the worker-side loop before teardown so its rAF/setTimeout chain
    // stops rescheduling and node:test's event loop can drain.
    adoption.pause();
    await new Promise((r) => setTimeout(r, 20));
    adoption.dispose(); h.destroy();
  } finally {
    globalThis.requestAnimationFrame = savedRaf;
    globalThis.cancelAnimationFrame = savedCaf;
  }
});

// --- 25. SAB mode: negotiation, seqlock publish, latest-wins --------------
test("25. SAB mode: negotiation, seqlock publish, latest-wins", { skip: !SAB_OK }, async () => {
  const stride = 8, capacity = 4;
  const [pt, ct] = pairTyped();
  const p = frameChannel(pt, { stride, capacity }, { role: "producer" });
  const c = frameChannel(ct, { stride, capacity }, { role: "consumer" });
  assert.ok(p.mode === "shared" && p.shared === true, "producer negotiates shared mode when SAB is available");
  await tick();
  assert.ok(c.mode === "shared" && c.shared === true, "consumer upgrades to shared on handshake");

  assert.ok(c.read() === null, "read() is null before the first publish");
  p.produce((f) => { f[0] = 11; f[stride] = 22; });
  const v = c.read();
  assert.ok(!!v && v[0] === 11 && v[stride] === 22, "shared publish is visible to the consumer");
  assert.ok(p.produce(() => {}) === true && p.free === p.count, "produce() never starves in shared mode");

  for (let i = 0; i < 500; i++) p.produce((f) => { f[0] = i; });
  const latest = c.read();
  assert.ok(latest[0] === 499, "consumer sees the newest frame after a burst");
  assert.ok(c.dropped > 0, "unread frames count as dropped (latest-wins)");

  const dst = new Float32Array(stride * capacity);
  p.produce((f) => { f[0] = 77; });
  assert.ok(c.readInto(dst) === true && dst[0] === 77, "readInto() takes a tear-free snapshot");
  assert.ok(typeof c.torn === "number" && c.torn >= 0, "torn counter exposed and sane");
  p.dispose(); c.dispose();
});

// --- 26. SAB mode: explicit selection + graceful fallback ------------------
test("26. SAB mode: explicit selection + graceful fallback", async () => {
  const [pt, ct] = pairTyped();
  assert.ok(frameChannel(pt, 4, { role: "producer", capacity: 4, mode: "transfer" }).mode === "transfer",
    "mode:'transfer' stays on the transferable ring even when SAB exists");
  assert.ok(await throws(() => frameChannel(pt, 4, { role: "producer", capacity: 4, mode: "nope" }), "mode"),
    "invalid mode throws");

  // A bare { send, onRaw } transport can't negotiate the handover, so shared
  // mode is unavailable and auto falls back silently.
  const [bare] = pair();
  assert.ok(frameChannel(bare, 4, { role: "producer", capacity: 4 }).mode === "transfer",
    "auto falls back to transfer on a transport without post()/on()");
  assert.ok(await throws(() => frameChannel(bare, 4, { role: "producer", capacity: 4, mode: "shared" }), "post()"),
    "explicit mode:'shared' on a bare transport throws");

  // Layout mismatch must not silently attach the wrong shared region.
  if (SAB_OK) {
    const [a, b] = pairTyped();
    const prod = frameChannel(a, { stride: 4, capacity: 8 }, { role: "producer" });
    const mism = frameChannel(b, { stride: 4, capacity: 9 }, { role: "consumer" });
    await tick();
    assert.ok(mism.mode === "transfer", "consumer ignores a handshake whose layout doesn't match");
    prod.dispose(); mism.dispose();
  }
  void ct;
});

// --- 27. W-10: fail-closed lw:fc:sab handshake ------------------------------
test("27. W-10: spoofed ArrayBuffer handshake stays on the ring; a real SharedArrayBuffer attaches", { skip: !SAB_OK }, async () => {
  const stride = 4, capacity = 4, count = 2;
  const byteLength = capacity * stride * 4;
  const meta = (sab) => ({ sab, stride, capacity, kind: "f32", count, byteLength });

  // Positive control: a real SharedArrayBuffer with a matching layout DOES
  // attach through the identical path -- proves the attach mechanism is live
  // so the negative control below isn't meaningless.
  {
    const [pt, ct] = pairTyped();
    const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode: "auto", count });
    assert.equal(cons.mode, "transfer", "consumer starts on the ring before any handshake");
    pt.post("lw:fc:sab", meta(new SharedArrayBuffer(16 + count * byteLength)));
    await tick();
    assert.equal(cons.mode, "shared", "a real SharedArrayBuffer handshake upgrades the consumer");
    assert.equal(cons.shared, true, "shared flag is true after a real attach");
    cons.dispose();
  }

  // Negative control (W-10): a spoofed plain ArrayBuffer carrying a matching
  // byteLength/count must be rejected by the `instanceof SharedArrayBuffer`
  // guard -- the consumer stays on the transferable ring.
  {
    const [pt, ct] = pairTyped();
    const cons = frameChannel(ct, { stride, capacity }, { role: "consumer", mode: "auto", count });
    pt.post("lw:fc:sab", meta(new ArrayBuffer(16 + count * byteLength)));
    await tick();
    assert.equal(cons.mode, "transfer", "a spoofed plain ArrayBuffer handshake does NOT attach (W-10 fail-closed)");
    assert.equal(cons.shared, false, "shared flag stays false after a spoofed handshake");
    cons.dispose();
  }
});

// --- 28. W-06: live-view boundary --------------------------------------------
// read()'s view stays coherent for exactly count-1 further produce() calls and
// is overwritten on the count-th, at the documented default count=2 -- mirrors
// the ROADMAP W-06 reproduction (count=2: held view 100 -> 300 after 2
// publishes) in both transfer and shared mode.
test("28. W-06: live-view coherent for count-1 publishes, overwritten on the count-th", async () => {
  // Transfer mode (default transport needs no typed plane).
  {
    const count = 2;
    const [pt, ct] = pair();
    const prod = frameChannel(pt, { stride: 4, capacity: 1 }, { role: "producer", mode: "transfer", count });
    const cons = frameChannel(ct, { stride: 4, capacity: 1 }, { role: "consumer", mode: "transfer", count });
    prod.produce((f) => { f[0] = 100; }); await tick();
    const held = cons.read();
    assert.equal(held[0], 100, "transfer: initial live view holds the first published value");
    prod.produce((f) => { f[0] = 200; }); await tick(); // count-1 = 1 further publish
    assert.equal(held[0], 100, "transfer: live view still coherent after count-1 further publishes");
    prod.produce((f) => { f[0] = 300; }); await tick(); // count-th further publish
    assert.equal(held[0], 300, "transfer: live view overwritten on the count-th further publish");
    prod.dispose(); cons.dispose();
  }

  // Shared mode: same boundary, over live shared memory.
  if (SAB_OK) {
    const count = 2;
    const [pt, ct] = pairTyped();
    const prod = frameChannel(pt, { stride: 4, capacity: 1 }, { role: "producer", mode: "shared", count });
    const cons = frameChannel(ct, { stride: 4, capacity: 1 }, { role: "consumer", mode: "shared", count });
    await tick();
    assert.equal(cons.mode, "shared", "shared negotiation succeeded before the boundary check");
    prod.produce((f) => { f[0] = 100; });
    const held = cons.read();
    assert.equal(held[0], 100, "shared: initial live view holds the first published value");
    prod.produce((f) => { f[0] = 200; }); // count-1 = 1 further publish
    assert.equal(held[0], 100, "shared: live view still coherent after count-1 further publishes");
    prod.produce((f) => { f[0] = 300; }); // count-th further publish
    assert.equal(held[0], 300, "shared: live view overwritten on the count-th further publish");
    prod.dispose(); cons.dispose();
  }
});
