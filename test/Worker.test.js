// Loopback test harness. Mocks Blob/URL/Worker so the *actual* serialized
// worker runtime (and the user module fn) runs in-process, wired to the
// main-side handle through microtask-queued postMessage in both directions.
// This exercises real code on both sides — envelope discrimination, reply
// correlation, raw transport, lifecycle — without a browser.

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

const { defineWorker, frameChannel } = await import("../Worker.js");

let pass = 0, fail = 0;
const results = [];
function ok(name, cond) {
  if (cond) { pass++; results.push("  ok   " + name); }
  else { fail++; results.push("  FAIL " + name); }
}
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

// --- 1. lifecycle basics ---------------------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("noop", () => {}); });
  ok("not spawned before spawn()", h.spawned === false);
  const same = h.spawn();
  ok("spawn() returns handle", same === h);
  ok("spawned after spawn()", h.spawned === true);
  ok("spawn() idempotent", h.spawn() === h && h.spawned === true);
  h.destroy();
}

// --- 2. post -> worker -> push back ----------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("ping", (n) => ctx.post("pong", n + 1)); }).spawn();
  const p = once(h, "pong");
  h.post("ping", 41);
  ok("post round-trips through worker push", (await p) === 42);
  h.destroy();
}

// --- 3. call: explicit reply -----------------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("add", (d, reply) => reply(d.a + d.b)); }).spawn();
  ok("call() resolves with explicit reply", (await h.call("add", { a: 2, b: 3 })) === 5);
  h.destroy();
}

// --- 4. call: sync return auto-reply ---------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("mul", (d) => d.a * d.b); }).spawn();
  ok("call() auto-replies from sync return", (await h.call("mul", { a: 4, b: 5 })) === 20);
  h.destroy();
}

// --- 5. call: async return auto-reply --------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("slow", async (d) => d * 2); }).spawn();
  ok("call() auto-replies from async return", (await h.call("slow", 21)) === 42);
  h.destroy();
}

// --- 6. call: handler throws -> reject --------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("boom", () => { throw new Error("kaboom"); }); }).spawn();
  ok("call() rejects when handler throws", await throws(() => h.call("boom"), "kaboom"));
  h.destroy();
}

// --- 7. call: no handler -> reject ------------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("known", () => 1); }).spawn();
  ok("call() rejects with no handler", await throws(() => h.call("unknown"), "no handler"));
  h.destroy();
}

// --- 8. call: timeout -------------------------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("hang", () => undefined); }).spawn();
  ok("call() times out", await throws(() => h.call("hang", null, { timeout: 40 }), "timed out"));
  h.destroy();
}

// --- 9. raw transport both directions --------------------------------------
{
  const h = defineWorker((ctx) => {
    ctx.onRaw((buf) => { const v = new Uint8Array(buf.buffer || buf); v[0] = v[0] + 1; ctx.send(v); });
  }).spawn();
  const p = onceRaw(h);
  h.send(new Uint8Array([7]));
  const got = await p;
  ok("raw send()/onRaw() round-trips", new Uint8Array(got.buffer || got)[0] === 8);
  h.destroy();
}

// --- 10. unsubscribe --------------------------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("e", () => ctx.post("f", 1)); }).spawn();
  let hits = 0;
  const off = h.on("f", () => { hits++; });
  h.post("e"); await tick();
  off();
  h.post("e"); await tick();
  ok("on() unsubscribe stops delivery", hits === 1);
  h.destroy();
}

// --- 11. terminate rejects pending, handle reusable ------------------------
{
  const h = defineWorker((ctx) => { ctx.on("hang", () => undefined); }).spawn();
  const p = h.call("hang");
  const rejected = throws(() => p, "terminated");
  h.terminate();
  ok("terminate() rejects pending calls", await rejected);
  ok("terminate() leaves handle re-spawnable", h.spawned === false && h.destroyed === false);
  h.spawn();
  ok("re-spawn() works after terminate()", h.spawned === true);
  h.destroy();
}

// --- 12. destroy idempotent + guards ---------------------------------------
{
  const h = defineWorker((ctx) => { ctx.on("x", () => {}); }).spawn();
  h.destroy();
  let threw = false;
  try { h.destroy(); } catch { threw = true; }
  ok("destroy() is idempotent", threw === false && h.destroyed === true);
  ok("spawn() after destroy throws", await throws(() => { h.spawn(); }, "destroyed"));
  ok("post() after destroy is a no-op", (() => { h.post("x", 1); return true; })());
  ok("send() after destroy is a no-op", (() => { h.send(new Uint8Array([1])); return true; })());
  ok("call() after destroy rejects", await throws(() => h.call("x"), "destroyed"));
}

// --- 13. defineWorker guards ------------------------------------------------
{
  let threw = false;
  try { defineWorker(123); } catch (e) { threw = e instanceof TypeError; }
  ok("defineWorker(non-fn) throws TypeError", threw);
}

// --- 14. frameChannel: validation ------------------------------------------
{
  const [t] = pair();
  ok("frameChannel bad role throws", await throws(() => frameChannel(t, 8, { role: "x", capacity: 4 }), "role"));
  ok("frameChannel bare stride needs capacity", await throws(() => frameChannel(t, 8, { role: "producer" }), "capacity"));
  ok("frameChannel zero stride throws", await throws(() => frameChannel(t, 0, { role: "producer", capacity: 4 }), "stride"));
  ok("frameChannel garbage layout throws", await throws(() => frameChannel(t, "nope", { role: "producer" })));
}

// --- 15. frameChannel: lite-gl LAYOUT interop shape ------------------------
{
  const [a, b] = pair();
  const cap = 100, stride = 8; // lite-gl LAYOUT.POINT
  const p = frameChannel(a, stride, { role: "producer", capacity: cap });
  const c = frameChannel(b, { stride, capacity: cap }, { role: "consumer" });
  ok("byteLength = capacity*stride*4", p.byteLength === cap * stride * 4);
  ok("stride/capacity/kind exposed", p.stride === 8 && p.capacity === cap && p.kind === "f32");
  ok("count defaults to 2", p.count === 2 && c.count === 2);
  ok("{bytes} layout -> byte kind", frameChannel(a, { bytes: 64 }, { role: "producer" }).kind === "bytes");
  p.dispose(); c.dispose();
}

// --- 16. frameChannel: data round-trip (in-process) ------------------------
{
  const [a, b] = pair();
  const stride = 4, cap = 3;
  const p = frameChannel(a, stride, { role: "producer", capacity: cap });
  const c = frameChannel(b, stride, { role: "consumer", capacity: cap });
  ok("read() is null before first frame", c.read() === null);
  p.produce((f) => { f[0] = 7; f[stride] = 9; });
  await tick();
  const v = c.read();
  ok("consumer reads produced values", !!v && v[0] === 7 && v[stride] === 9);
  ok("read() view is cached (same object across reads)", c.read() === v);
  p.dispose(); c.dispose();
}

// --- 17. frameChannel: worker-side ctx.frameChannel (serialized copy) ------
{
  const h = defineWorker((ctx) => {
    // self-contained: constants inlined, nothing closed over
    const ch = ctx.frameChannel(4, { role: "producer", capacity: 2 });
    let seq = 0;
    ctx.on("tick", () => { ch.produce((f) => { f[0] = ++seq; }); });
  }).spawn();
  const c = h.frameChannel({ stride: 4, capacity: 2 }, { role: "consumer" });
  h.post("tick"); await tick(); await tick();
  const v = c.read();
  ok("worker ctx.frameChannel produces to main consumer", !!v && v[0] === 1);
  h.post("tick"); h.post("tick"); await tick(); await tick();
  const v2 = c.read();
  ok("consumer advances to latest worker frame", !!v2 && v2[0] >= 2);
  h.destroy();
}

// --- 18. frameChannel: latest-wins drop ------------------------------------
{
  const [a, b] = pair();
  const p = frameChannel(a, 4, { role: "producer", capacity: 2, count: 2 });
  const c = frameChannel(b, 4, { role: "consumer", capacity: 2, count: 2 });
  p.produce((f) => { f[0] = 1; }); await tick(); // arrives, left unread
  p.produce((f) => { f[0] = 2; }); await tick(); // supersedes the unread frame -> drop
  ok("dropped increments when a frame is superseded unread", c.dropped >= 1);
  ok("consumer holds the latest, not the dropped frame", c.read()[0] === 2);
  p.dispose(); c.dispose();
}

// --- 19. frameChannel: bounded — pool conserved, never queues --------------
{
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
  ok("producer.free never exceeds the pool count", maxFree <= count);
  ok("every round is accounted (produced+dropped)", produced + dropped === 3000);
  ok("frames dropped under flood (bounded, not queued)", dropped > 0);
  ok("consumer still has a valid latest frame after soak", c.read() !== null);
  p.dispose(); c.dispose();
}

// --- 20. frameChannel: dispose detaches ------------------------------------
{
  const [a, b] = pair();
  const p = frameChannel(a, 2, { role: "producer", capacity: 4 });
  const c = frameChannel(b, 2, { role: "consumer", capacity: 4 });
  p.dispose(); c.dispose();
  ok("produce() after dispose returns false", p.produce(() => {}) === false);
  ok("dispose() is idempotent", (() => { p.dispose(); c.dispose(); return true; })());
}

// --- 21. adoptCanvas + ctx.onCanvas (protocol over the loopback) -----------
{
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

  ok("adoptCanvas throws without transferControlToOffscreen",
    await throws(() => h.adoptCanvas({}), "transferControlToOffscreen"));

  const fakeCanvas = {
    width: 0, height: 0,
    getBoundingClientRect: () => ({ width: 300, height: 150 }),
    transferControlToOffscreen: () => ({ width: 0, height: 0 })
  };
  const adoption = h.adoptCanvas(fakeCanvas);
  await tick();
  ok("onCanvas receives device dims (dpr applied)", got.adopted && got.adopted.w === 600 && got.adopted.h === 300);
  ok("ctl exposes dpr + frame + visible", got.adopted && got.adopted.dpr === 2 && got.adopted.hasFrame && got.adopted.visible === true);

  adoption.resize(); await tick();
  ok("resize is forwarded to the worker", got.resized && got.resized.cw === 600 && got.resized.dpr === 2);

  adoption.pause(); await tick();
  ok("pause forwards visibility=false", got.vis && got.vis.v === false);
  adoption.resume(); await tick();
  ok("resume forwards visibility=true", got.vis && got.vis.v === true);

  await new Promise((r) => setTimeout(r, 50)); // let the ~120fps worker timer fire
  ok("frame() render loop fires (timer-driven, auto-paused when hidden)", drew > 0);

  adoption.dispose();
  ok("adoption.dispose() does not throw", true);
  h.destroy();
}

// --- 22. spawn/destroy leak gate: no orphaned Blob URLs --------------------
{
  const before = sources.size;
  for (let i = 0; i < 50; i++) {
    const h = defineWorker((ctx) => { ctx.on("noop", () => {}); });
    h.spawn();
    h.terminate();
    h.destroy();
  }
  ok("no Blob URLs retained across 50 spawn/destroy cycles", sources.size === before);
  const h = defineWorker(() => {}).spawn();
  ok("spawned worker holds no lingering object URL (revoked on construction)", sources.size === before);
  h.destroy();
  ok("destroy() marks the handle destroyed", h.destroyed === true);
}

// --- 23. worker error rejects in-flight calls (no hang to timeout) ---------
{
  const h = defineWorker((ctx) => {
    ctx.on("hang", () => new Promise(() => {})); // never resolves
  }).spawn();
  const p = h.call("hang", null, { timeout: 60000 }); // long timeout; must NOT wait for it
  await tick();
  // simulate a worker load/parse/uncaught failure
  h._worker.onerror({ message: "simulated worker failure" });
  let rejected = false, msg = "";
  try { await p; } catch (e) { rejected = true; msg = e.message; }
  ok("worker error rejects the pending call immediately", rejected && /simulated worker failure/.test(msg));
  ok("pending map cleared after error", h._pending.size === 0);
  h.destroy();
}

// --- 24. render loop prefers requestAnimationFrame when available ----------
{
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
    ok("frame() without an explicit rate uses requestAnimationFrame", rafCalls > 0);
    ok("rAF-driven loop actually renders", drew > 0);
    adoption.dispose(); h.destroy();
  } finally {
    globalThis.requestAnimationFrame = savedRaf;
    globalThis.cancelAnimationFrame = savedCaf;
  }
}

console.log(results.join("\n"));
console.log("\n" + pass + " passed, " + fail + " failed");
process.exit(fail ? 1 : 0);
