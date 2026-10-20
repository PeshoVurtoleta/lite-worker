// test/mailbox.test.js -- unit tests for the mailbox() SPSC lane
// (shared path). Written from the B1 spec, not from the implementation.
// Expected values come from the spec's layout table and from hand arithmetic
// noted next to each case.

import { test } from "node:test";
import assert from "node:assert/strict";
import { syncPair } from "./torture/harness.mjs";

const NS = await import("../Worker.js");
const { mailbox, defineWorker } = NS;
const T = { timeout: 30000 };
const ERR = (e) => e instanceof Error && e.message.startsWith("lite-worker: mailbox");

const HEAD = 0, ARMED = 1, TAIL = 32, FBN = 33, GEN = 64, STATE = 65,
  CAPW = 66, WW = 67, TYPEW = 68, SLOTS = 69, SLOT_BYTES = 70;
const r128 = (n) => (n + 127) & ~127;

// Producer on a, consumer on b; the sab message is captured on b.
function rig(cfg, opts) {
  const [a, b] = syncPair(true);
  const cap = { sab: null, msg: null };
  b.on("lw:mb:sab", (m) => { cap.sab = m.sab; cap.msg = m; });
  const prod = mailbox(a, cfg, { role: "producer", mode: "shared", ...(opts && opts.p) });
  const recs = [];
  const arrOf = (arr, off, W) => { const r = []; for (let k = 0; k < W; k++) r.push(arr[off + k]); return r; };
  const W = cfg.words;
  const cons = mailbox(b, cfg, {
    role: "consumer", mode: "shared",
    onRecord: (arr, off) => { recs.push(arrOf(arr, off, W)); },
    ...(opts && opts.c),
  });
  return { a, b, prod, cons, recs, cap, i32: new Int32Array(cap.sab) };
}

test("statics: outcomes present and non-writable", T, () => {
  const exp = { OK: 0, FULL: 1, SLAB_EXHAUSTED: 2, SLOT_TOO_SMALL: 3, RETIRED: 4, REJECTED: 5 };
  for (const k of Object.keys(exp)) {
    assert.equal(mailbox[k], exp[k], k);
    try { mailbox[k] = 99; } catch (e) { /* strict-mode TypeError is fine */ }
    assert.equal(mailbox[k], exp[k], k + " unchanged after assignment");
  }
});

test("layout: byteLength and cold words, i32 and f64", T, () => {
  const combos = [
    ["i32", 8, 3], ["i32", 1, 1], ["f64", 4, 5], ["f64", 2, 1],
  ];
  for (const [type, cap, w] of combos) {
    const { i32, cap: c } = (() => { const r = rig({ capacity: cap, words: w, type }); return { i32: r.i32, cap: r.cap }; })();
    const elt = type === "f64" ? 8 : 4;
    assert.equal(c.sab.byteLength, 384 + r128(cap * w * elt) + 128, type + cap + "x" + w);
    assert.equal(Atomics.load(i32, GEN), 1);
    assert.equal(Atomics.load(i32, STATE), 0);
    assert.equal(Atomics.load(i32, CAPW), cap);
    assert.equal(Atomics.load(i32, WW), w);
    assert.equal(Atomics.load(i32, TYPEW), type === "f64" ? 1 : 0);
    assert.equal(Atomics.load(i32, SLOTS), 0);
    assert.equal(Atomics.load(i32, SLOT_BYTES), 0);
    assert.equal(Atomics.load(i32, FBN), 0);
    assert.equal(c.msg.gen, 1);
    assert.equal(c.msg.id, 0);
  }
});

test("FIFO: fill to CAP, FULL, drain all, push OK", T, () => {
  const CAP = 4, r = rig({ capacity: CAP, words: 2 });
  for (let i = 0; i < CAP; i++) assert.equal(r.prod.push([i, i * 10], 0), 0);
  assert.equal(r.prod.push([99, 99], 0), 1);
  assert.equal(r.cons.drain(), CAP);
  assert.deepEqual(r.recs, [[0, 0], [1, 10], [2, 20], [3, 30]]);
  assert.equal(r.cons.drain(), 0);
  assert.equal(r.prod.push([7, 8], 0), 0);
  assert.equal(r.cons.drain(), 1);
  assert.deepEqual(r.recs[4], [7, 8]);
});

test("offset: push copies src[off .. off+W)", T, () => {
  const r = rig({ capacity: 2, words: 3 });
  assert.equal(r.prod.push([9, 9, 1, 2, 3, 9], 2), 0);
  r.cons.drain();
  assert.deepEqual(r.recs[0], [1, 2, 3]);
});

test("interleaved push/drain for 3*CAP+5 records", T, () => {
  const CAP = 8, N = 3 * CAP + 5, r = rig({ capacity: CAP, words: 2 });
  const src = new Int32Array(2);
  let sent = 0, got = 0;
  // deterministic burst pattern: burst sizes 1..5 cycling, never over CAP
  let burst = 1;
  while (sent < N) {
    const n = Math.min(burst, N - sent);
    for (let k = 0; k < n; k++) { src[0] = sent; src[1] = ~sent; assert.equal(r.prod.push(src, 0), 0); sent++; }
    got += r.cons.drain();
    burst = burst % 5 + 1;
  }
  got += r.cons.drain();
  assert.equal(got, N);
  assert.equal(r.recs.length, N);
  for (let i = 0; i < N; i++) assert.deepEqual(r.recs[i], [i, ~i]);
});

test("wrap at 2^31: compares are (a - b) | 0", T, () => {
  const CAP = 4, [a, b] = syncPair(true);
  let sab = null; b.on("lw:mb:sab", (m) => { sab = m.sab; });
  const prod = mailbox(a, { capacity: CAP, words: 1 }, { role: "producer", mode: "shared" });
  const i32 = new Int32Array(sab);
  Atomics.store(i32, HEAD, 0x7ffffffe); Atomics.store(i32, TAIL, 0x7ffffffe);
  const seen = [], heads = [], pos0 = 0x7ffffffe;
  const cons = mailbox(b, { capacity: CAP, words: 1 }, {
    role: "consumer", mode: "shared",
    onRecord: (arr, off) => { seen.push(arr[off]); heads.push(Atomics.load(i32, HEAD)); },
  });
  for (let i = 0; i < CAP; i++) assert.equal(prod.push([100 + i], 0), 0, "push " + i);
  assert.equal(prod.push([555], 0), 1, "FULL across the wrap");
  assert.equal(cons.drain(), CAP);
  assert.deepEqual(seen, [100, 101, 102, 103]);
  // positions pos0, pos0+1, pos0+2, pos0+3 as int32 (wrap past 0x7fffffff)
  assert.deepEqual(heads, [0, 1, 2, 3].map((k) => (pos0 + k) | 0));
  assert.equal(prod.push([777], 0), 0);
  assert.equal(cons.drain(), 1);
  assert.equal(seen[4], 777);
});

test("HEAD is stored after each callback returns", T, () => {
  const [a, b] = syncPair(true);
  let sab = null; b.on("lw:mb:sab", (m) => { sab = m.sab; });
  const prod = mailbox(a, { capacity: 4, words: 1 }, { role: "producer", mode: "shared" });
  const i32 = new Int32Array(sab), heads = [];
  const cons = mailbox(b, { capacity: 4, words: 1 }, {
    role: "consumer", mode: "shared", onRecord: () => { heads.push(Atomics.load(i32, HEAD)); },
  });
  for (let i = 0; i < 3; i++) prod.push([i], 0);
  cons.drain();
  assert.deepEqual(heads, [0, 1, 2]);
  assert.equal(Atomics.load(i32, HEAD), 3);
});

test("ARMED notify: 0 -> no calls; 1 -> one per push at index 32", T, () => {
  const r = rig({ capacity: 16, words: 1 });
  const orig = globalThis.Atomics.notify, calls = [];
  globalThis.Atomics.notify = function (ta, idx, cnt) { calls.push([ta, idx, cnt]); return 0; };
  try {
    for (let i = 0; i < 5; i++) r.prod.push([i], 0);
    assert.equal(calls.length, 0, "ARMED 0");
    Atomics.store(r.i32, ARMED, 1);
    for (let i = 0; i < 5; i++) r.prod.push([i], 0);
    assert.equal(calls.length, 5, "one per push");
    for (const c of calls) assert.equal(c[1], TAIL);
  } finally { globalThis.Atomics.notify = orig; }
});

test("i32 lanes: ToInt32, no validation", T, () => {
  const vals = [1.9, -1.9, 2 ** 32 + 5, NaN, Infinity];
  const want = [1, -1, 5, 0, 0];
  const r = rig({ capacity: 8, words: 1 });
  for (const v of vals) assert.equal(r.prod.push([v], 0), 0);
  r.cons.drain();
  assert.deepEqual(r.recs.map((x) => x[0]), want);
});

test("f64 lanes: -0, 0.1, 1e308 exact", T, () => {
  const r = rig({ capacity: 4, words: 3, type: "f64" });
  r.prod.push([-0, 0.1, 1e308], 0);
  r.cons.drain();
  assert.ok(Object.is(r.recs[0][0], -0));
  assert.equal(r.recs[0][1], 0.1);
  assert.equal(r.recs[0][2], 1e308);
});

test("consumer fails closed on bad sab messages", T, () => {
  const cfg = { capacity: 4, words: 2 };
  const good = rig(cfg);
  const goodSab = good.cap.sab;
  const goodGen = good.cap.msg.gen;
  const elt = 4;
  const size = 384 + r128(cfg.capacity * cfg.words * elt) + 128;
  function fake(mut, sz = size, Ctor = SharedArrayBuffer) {
    const s = new Ctor(sz), v = new Int32Array(s);
    v[GEN] = 1; v[CAPW] = cfg.capacity; v[WW] = cfg.words; v[TYPEW] = 0;
    mut(v); return s;
  }
  const cases = {
    "plain ArrayBuffer": { id: 0, gen: 1, sab: fake(() => {}, size, ArrayBuffer) },
    "wrong byteLength": { id: 0, gen: 1, sab: fake(() => {}, size + 128) },
    "STATE retired": { id: 0, gen: 1, sab: fake((v) => { v[STATE] = 1; }) },
    "SLOTS nonzero": { id: 0, gen: 1, sab: fake((v) => { v[SLOTS] = 1; }) },
    "SLOT_BYTES nonzero": { id: 0, gen: 1, sab: fake((v) => { v[SLOT_BYTES] = 8; }) },
    "CAP mismatch": { id: 0, gen: 1, sab: fake((v) => { v[CAPW] = 8; }) },
    "W mismatch": { id: 0, gen: 1, sab: fake((v) => { v[WW] = 3; }) },
    "TYPE mismatch": { id: 0, gen: 1, sab: fake((v) => { v[TYPEW] = 1; }) },
    "GEN != msg.gen": { id: 0, gen: goodGen + 1, sab: goodSab },
    "mismatched id": { id: 1, gen: goodGen, sab: goodSab },
  };
  for (const name of Object.keys(cases)) {
    const [a, b] = syncPair(true);
    const got = [];
    const cons = mailbox(b, cfg, { role: "consumer", mode: "shared", onRecord: (x, o) => got.push(x[o]) });
    a.post("lw:mb:sab", cases[name]);
    for (let i = 0; i < 3; i++) good.prod.push([i, i], 0);
    assert.equal(cons.drain(), 0, name);
    assert.equal(got.length, 0, name);
    assert.equal(cons.attached, false, name);
  }
  // must-pass control: the same fake, unmutated, attaches (each case differs in one field only)
  const [ca, cb] = syncPair(true);
  const ctl = mailbox(cb, cfg, { role: "consumer", mode: "shared", onRecord: () => {} });
  ca.post("lw:mb:sab", { id: 0, gen: 1, sab: fake(() => {}) });
  assert.equal(ctl.attached, true, "valid fake must attach");
});

test("id: duplicate throws, reusable after dispose, distinct ids isolate", T, () => {
  const [a, b] = syncPair(true);
  const cfg = { capacity: 4, words: 1 };
  const p0 = mailbox(a, cfg, { role: "producer", mode: "shared", id: 0 });
  assert.throws(() => mailbox(a, cfg, { role: "producer", mode: "shared", id: 0 }), ERR);
  p0.dispose();
  const p0b = mailbox(a, cfg, { role: "producer", mode: "shared", id: 0 });
  const p1 = mailbox(a, cfg, { role: "producer", mode: "shared", id: 1 });
  const g0 = [], g1 = [];
  const c0 = mailbox(b, cfg, { role: "consumer", mode: "shared", id: 0, onRecord: (x, o) => g0.push(x[o]) });
  const c1 = mailbox(b, cfg, { role: "consumer", mode: "shared", id: 1, onRecord: (x, o) => g1.push(x[o]) });
  p0b.push([10], 0); p0b.push([11], 0); p1.push([20], 0);
  assert.equal(c0.drain(), 2); assert.equal(c1.drain(), 1);
  assert.deepEqual(g0, [10, 11]); assert.deepEqual(g1, [20]);
  c1.dispose();
  const c1b = mailbox(b, cfg, { role: "consumer", mode: "shared", id: 1, onRecord: () => {} });
  assert.ok(c1b);
});

test("dispose: producer push -> RETIRED, STATE 1; consumer drain -> 0", T, () => {
  const r = rig({ capacity: 4, words: 1 });
  r.prod.push([1], 0);
  r.prod.dispose();
  assert.equal(r.prod.push([2], 0), 4);
  assert.equal(Atomics.load(r.i32, STATE), 1);
  r.cons.dispose();
  assert.equal(r.cons.drain(), 0);
});

test("drain returns 0 before attach", T, () => {
  const [, b] = syncPair(true);
  const c = mailbox(b, { capacity: 4, words: 1 }, { role: "consumer", mode: "shared", onRecord: () => {} });
  assert.equal(c.drain(), 0);
});

test("onRecord throw propagates; record redelivered next drain", T, () => {
  const [a, b] = syncPair(true);
  const cfg = { capacity: 4, words: 1 };
  const prod = mailbox(a, cfg, { role: "producer", mode: "shared" });
  const seen = []; let boom = true;
  const cons = mailbox(b, cfg, {
    role: "consumer", mode: "shared",
    onRecord: (x, o) => { seen.push(x[o]); if (x[o] === 2 && boom) { boom = false; throw new Error("boom"); } },
  });
  for (let i = 1; i <= 3; i++) prod.push([i], 0);
  assert.throws(() => cons.drain(), /boom/);
  assert.deepEqual(seen, [1, 2]);
  assert.equal(cons.drain(), 2);
  assert.deepEqual(seen, [1, 2, 2, 3]);
});

test("nested drain inside onRecord returns 0", T, () => {
  const [a, b] = syncPair(true);
  const cfg = { capacity: 4, words: 1 };
  const prod = mailbox(a, cfg, { role: "producer", mode: "shared" });
  const inner = []; let cons;
  cons = mailbox(b, cfg, { role: "consumer", mode: "shared", onRecord: () => { inner.push(cons.drain()); } });
  prod.push([1], 0); prod.push([2], 0);
  assert.equal(cons.drain(), 2);
  assert.deepEqual(inner, [0, 0]);
});

test("construction errors", T, () => {
  const [a, b] = syncPair(true);
  const ok = { capacity: 4, words: 1 };
  const P = { role: "producer", mode: "shared" };
  const bad = [
    [{ ...ok, capacity: 0 }, P], [{ ...ok, capacity: 3 }, P],
    [{ ...ok, words: 0 }, P], [{ ...ok, words: 1.5 }, P],
    [{ ...ok, type: "u8" }, P],
    [ok, { mode: "shared" }],
    [ok, { role: "producer" }],
    [ok, { role: "producer", mode: "auto" }],
    [ok, { role: "producer", mode: "transfer" }],
    [ok, { role: "consumer", mode: "shared" }],
    [ok, { ...P, id: -1 }], [ok, { ...P, id: 1.5 }],
  ];
  for (let i = 0; i < bad.length; i++) {
    assert.throws(() => mailbox(i % 2 ? a : b, bad[i][0], bad[i][1]), ERR, "case " + i);
  }
});

test("integration: handle prototype method and export keys", T, () => {
  const h = defineWorker(() => {});
  const proto = Object.getPrototypeOf(h);
  assert.equal(typeof proto.mailbox, "function");
  assert.deepEqual(Object.keys(NS).sort(), ["VERSION", "default", "defineWorker", "frameChannel", "mailbox"]);
  h.destroy();
});

test("id registry keyed by (id, role): producer and consumer id 0 coexist", T, () => {
  const [a] = syncPair(true);
  const cfg = { capacity: 4, words: 1 };
  const p = mailbox(a, cfg, { role: "producer", mode: "shared", id: 0 });
  const c = mailbox(a, cfg, { role: "consumer", mode: "shared", id: 0, onRecord: () => {} });
  assert.throws(() => mailbox(a, cfg, { role: "producer", mode: "shared", id: 0 }), ERR);
  assert.throws(() => mailbox(a, cfg, { role: "consumer", mode: "shared", id: 0, onRecord: () => {} }), ERR);
  // dispose frees only that (id, role); the other role keeps working
  p.dispose();
  assert.equal(p.push([1], 0), 4);
  const p2 = mailbox(a, cfg, { role: "producer", mode: "shared", id: 0 });
  assert.equal(p2.push([2], 0), 0);
  assert.throws(() => mailbox(a, cfg, { role: "consumer", mode: "shared", id: 0, onRecord: () => {} }), ERR);
  assert.equal(c.drain(), 0); // c still live (no throw), nothing addressed to it
  c.dispose();
  mailbox(a, cfg, { role: "consumer", mode: "shared", id: 0, onRecord: () => {} });
});

test("two-way pair: producer+consumer id 0 on each side, no cross-talk", T, () => {
  const [a, b] = syncPair(true);
  const cfg = { capacity: 8, words: 1 };
  const gotA = [], gotB = [];
  const pA = mailbox(a, cfg, { role: "producer", mode: "shared", id: 0 });
  const cA = mailbox(a, cfg, { role: "consumer", mode: "shared", id: 0, onRecord: (x, o) => gotA.push(x[o]) });
  const cB = mailbox(b, cfg, { role: "consumer", mode: "shared", id: 0, onRecord: (x, o) => gotB.push(x[o]) });
  const pB = mailbox(b, cfg, { role: "producer", mode: "shared", id: 0 });
  for (let v = 11; v <= 15; v++) assert.equal(pA.push([v], 0), 0);
  for (let v = 21; v <= 25; v++) assert.equal(pB.push([v], 0), 0);
  assert.equal(cB.drain(), 5);
  assert.equal(cA.drain(), 5);
  assert.deepEqual(gotB, [11, 12, 13, 14, 15]);
  assert.deepEqual(gotA, [21, 22, 23, 24, 25]);
});

// R4: ctx.mailbox round-trip with a real worker (review fix)
test("worker side: ctx.mailbox round-trip and statics (serialized _source)", T, async () => {
  const h = defineWorker((ctx) => {
    const got = [];
    const st = ["OK", "FULL", "SLAB_EXHAUSTED", "SLOT_TOO_SMALL", "RETIRED", "REJECTED"].map((k) => ctx.mailbox[k]);
    const c = ctx.mailbox({ capacity: 8, words: 2 }, { role: "consumer", mode: "shared", id: 3, onRecord: (v, o) => { got.push(v[o], v[o + 1]); } });
    const p = ctx.mailbox({ capacity: 4, words: 1, type: "f64" }, { role: "producer", mode: "shared", id: 3 });
    ctx.on("go", () => {
      const n = c.drain();
      const r = [p.push(new Float64Array([0.5]), 0), p.push(new Float64Array([1.25]), 0)];
      ctx.post("res", { st, n, got, attached: c.attached, r });
    });
  });
  h.spawn();
  const back = [];
  const mc = h.mailbox({ capacity: 4, words: 1, type: "f64" }, { role: "consumer", mode: "shared", id: 3, onRecord: (v, o) => { back.push(v[o]); } });
  const mp = h.mailbox({ capacity: 8, words: 2 }, { role: "producer", mode: "shared", id: 3 });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(mp.push(new Int32Array([7, 8]), 0), 0);
  assert.equal(mp.push(new Int32Array([9, 10]), 0), 0);
  const res = await new Promise((resolve) => { h.on("res", resolve); h.post("go", null); });
  assert.deepEqual(res, { st: [0, 1, 2, 3, 4, 5], n: 2, got: [7, 8, 9, 10], attached: true, r: [0, 0] });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(mc.drain(), 2);
  assert.deepEqual(back, [0.5, 1.25]);
  mp.dispose(); mc.dispose(); h.destroy();
});

// order: copy before publish, ARMED re-loaded after the TAIL store (review fix)
test("push order: copy before TAIL store; ARMED loaded after it", () => {
  const [a, b] = syncPair(true);
  let sab = null; b.on("lw:mb:sab", (m) => { sab = m.sab; });
  const p = mailbox(a, { capacity: 4, words: 1 }, { role: "producer", mode: "shared" });
  const i32 = new Int32Array(sab), orig = Atomics.notify; let tailSeen = -1, calls = 0;
  const src = { get 0() { tailSeen = Atomics.load(i32, 32); Atomics.store(i32, 1, 1); return 7; } };
  Atomics.notify = function () { calls++; return 0; };
  try { assert.equal(p.push(src, 0), 0); } finally { Atomics.notify = orig; }
  assert.equal(tailSeen, 0, "TAIL still unpublished during the copy");
  assert.equal(calls, 1, "ARMED set mid-push is seen by the post-store re-load");
});

// ---- round 2 ----

// Transport fake that counts live listeners per type and can throw once.
function countingTransport(opts) {
  const o = opts || {};
  const t = { _l: new Map(), postThrows: o.postThrows || 0, onThrows: o.onThrows || 0, posts: [] };
  t.count = (type) => (t._l.get(type) ? t._l.get(type).size : 0);
  t.on = (type, fn) => {
    if (t.onThrows > 0) { t.onThrows--; throw new Error("on boom"); }
    let s = t._l.get(type); if (!s) { s = new Set(); t._l.set(type, s); }
    s.add(fn);
    return () => s.delete(fn);
  };
  t.post = (type, data) => {
    if (t.postThrows > 0) { t.postThrows--; throw new Error("post boom"); }
    t.posts.push(type);
  };
  return t;
}

test("R1: post() throwing once rolls back registry and listeners (both roles)", T, () => {
  const cfg = { capacity: 4, words: 1 };
  const roles = [
    ["producer", "lw:mb:hello", {}],
    ["consumer", "lw:mb:sab", { onRecord: () => {} }],
  ];
  for (const [role, type, extra] of roles) {
    const t = countingTransport({ postThrows: 1 });
    const o = { role, mode: "shared", id: 5, ...extra };
    assert.throws(() => mailbox(t, cfg, o), /post boom/, role);
    assert.equal(t.count(type), 0, role + " listener left behind");
    const m = mailbox(t, cfg, o); // same id and role must be free again
    assert.ok(m, role);
    m.dispose();
  }
});

test("R1: on() throwing once rolls back the registry (both roles)", T, () => {
  const cfg = { capacity: 4, words: 1 };
  for (const [role, extra] of [["producer", {}], ["consumer", { onRecord: () => {} }]]) {
    const t = countingTransport({ onThrows: 1 });
    const o = { role, mode: "shared", id: 5, ...extra };
    assert.throws(() => mailbox(t, cfg, o), /on boom/, role);
    assert.equal(t.count("lw:mb:hello") + t.count("lw:mb:sab"), 0, role);
    assert.ok(mailbox(t, cfg, o), role);
  }
});

test("R2: inherited layout/options keys are rejected (own keys only)", T, () => {
  const [a] = syncPair(true);
  const cfg = { capacity: 4, words: 1 };
  Object.prototype.type = "f64";
  try {
    assert.throws(() => mailbox(a, cfg, { role: "producer", mode: "shared", id: 40 }), /inherited/);
  } finally { delete Object.prototype.type; }
  Object.prototype.mode = "shared";
  try {
    assert.throws(() => mailbox(a, cfg, { role: "producer", id: 41 }), /inherited/);
  } finally { delete Object.prototype.mode; }
  // control: after cleanup the same calls are fine
  assert.ok(mailbox(a, cfg, { role: "producer", mode: "shared", id: 40 }));
});

test("T3: id range [0, 2^30 - 1]; per-role coexistence; duplicate throws", T, () => {
  const [a] = syncPair(true);
  const cfg = { capacity: 4, words: 1 };
  const mk = (role, id) => mailbox(a, cfg, { role, mode: "shared", id, ...(role === "consumer" ? { onRecord: () => {} } : {}) });
  assert.ok(mk("producer", 2 ** 30 - 1));
  for (const bad of [2 ** 30, -1, 1.5, "1"]) {
    assert.throws(() => mk("producer", bad), ERR, String(bad));
  }
  assert.ok(mk("producer", 7));
  assert.ok(mk("consumer", 7)); // same id, other role
  assert.throws(() => mk("producer", 7), /already live/);
  assert.throws(() => mk("consumer", 7), /already live/);
});

test("T4: drain fails closed on TAIL outside [HEAD, HEAD + CAP]", T, () => {
  for (const delta of ["over", "under"]) {
    const r = rig({ capacity: 4, words: 1 });
    r.prod.push([1], 0);
    assert.equal(r.cons.drain(), 1);
    const head = Atomics.load(r.i32, HEAD);
    Atomics.store(r.i32, TAIL, delta === "over" ? head + 4 + 1 : head - 1);
    r.recs.length = 0;
    assert.equal(r.cons.drain(), 0, delta);
    assert.equal(r.recs.length, 0, delta + " onRecord called");
    assert.equal(r.cons.attached, false, delta);
    assert.equal(Atomics.load(r.i32, STATE), 1, delta);
    assert.equal(r.cons.drain(), 0, delta + " later drain");
    assert.equal(Atomics.load(r.i32, HEAD), head, delta + " HEAD moved");
    const again = () => mailbox(r.b, { capacity: 4, words: 1 }, { role: "consumer", mode: "shared", onRecord() {} });
    assert.throws(again, /already live/, delta + " registry kept until dispose");
    r.cons.dispose();
    assert.ok(again(), delta + " id free after dispose");
  }
});

test("T4 control: TAIL = HEAD + CAP after CAP real pushes delivers CAP records", T, () => {
  const r = rig({ capacity: 4, words: 1 });
  for (let i = 0; i < 4; i++) assert.equal(r.prod.push([i], 0), 0);
  assert.equal(Atomics.load(r.i32, TAIL) - Atomics.load(r.i32, HEAD), 4);
  assert.equal(r.cons.drain(), 4);
  assert.equal(r.cons.attached, true);
  assert.equal(Atomics.load(r.i32, STATE), 0);
});

test("dispose() inside onRecord stops the drain after that record", T, () => {
  const [a, b] = syncPair(true);
  const cfg = { capacity: 8, words: 1 };
  const prod = mailbox(a, cfg, { role: "producer", mode: "shared" });
  const seen = []; let cons;
  cons = mailbox(b, cfg, { role: "consumer", mode: "shared", onRecord: (x, o) => { seen.push(x[o]); if (x[o] === 2) cons.dispose(); } });
  for (let i = 1; i <= 5; i++) prod.push([i], 0);
  assert.equal(cons.drain(), 2);
  assert.deepEqual(seen, [1, 2]);
  assert.equal(cons.drain(), 0);
  assert.deepEqual(seen, [1, 2]);
});

test("R1: producer post() throwing after a sync delivery retires STATE", T, () => {
  const [a, b] = syncPair(true);
  let sab = null; b.on("lw:mb:sab", (m) => { sab = m.sab; });
  const post0 = a.post.bind(a);
  a.post = (t, d) => { post0(t, d); throw new Error("post boom"); };
  assert.throws(() => mailbox(a, { capacity: 4, words: 1 }, { role: "producer", mode: "shared" }), /post boom/);
  assert.equal(Atomics.load(new Int32Array(sab), STATE), 1);
});

test("R1: a transport that replays lw:mb:sab inside on() attaches cleanly", T, () => {
  const [a, b] = syncPair(true);
  let msg = null; b.on("lw:mb:sab", (m) => { msg = m; });
  mailbox(a, { capacity: 4, words: 1 }, { role: "producer", mode: "shared" });
  const on0 = b.on.bind(b);
  b.on = (t, fn) => { const off = on0(t, fn); if (t === "lw:mb:sab") fn(msg); return off; };
  const c = mailbox(b, { capacity: 4, words: 1 }, { role: "consumer", mode: "shared", onRecord() {} });
  assert.equal(c.attached, true);
  assert.equal(b._t.get("lw:mb:sab").size, 1, "only the capture listener is left");
});

test("R1: on() returning a non-function throws and rolls back (both roles)", T, () => {
  for (const role of ["producer", "consumer"]) {
    const [a, b] = syncPair(true);
    const opts = { role, mode: "shared", ...(role === "consumer" ? { onRecord() {} } : {}) };
    const on0 = a.on.bind(a);
    a.on = (t, fn) => { on0(t, fn); return undefined; };
    assert.throws(() => mailbox(a, { capacity: 4, words: 1 }, opts), ERR, role);
    let posts = 0; b.on("lw:mb:sab", () => { posts++; });
    for (const s of a._t.values()) s.forEach((f) => f({ id: 0 }));
    assert.equal(posts, 0, role + ": a stranded listener is inert");
    a.on = on0;
    assert.ok(mailbox(a, { capacity: 4, words: 1 }, opts), role + ": id reusable");
  }
});

test("R1: a consumer whose on() installs then throws is disposed (handler inert)", T, () => {
  const [, b] = syncPair(true);
  const on0 = b.on.bind(b);
  b.on = (t, fn) => { on0(t, fn); if (t === "lw:mb:sab") throw new Error("on boom"); return () => {}; };
  assert.throws(() => mailbox(b, { capacity: 4, words: 1 }, { role: "consumer", mode: "shared", onRecord() {} }), /on boom/);
  const fns = [...b._t.get("lw:mb:sab")];
  assert.equal(fns.length, 1, "the unreachable listener is still installed");
  let reads = 0;
  const m = { get id() { reads++; return "x"; } };
  fns.forEach((f) => f(m));
  assert.equal(reads, 0, "a disposed handler short-circuits before reading m.id");
});

// ===========================================================================
// E6: a throwing off() is called once and frees the id
// (from .handoff/e6/spec.md ASSERTIONS A1-A6; each fails on the pre-E6 build)
// ===========================================================================

const CFG6 = { capacity: 4, words: 1 };
// A real producer on a throwaway pair yields a valid lw:mb:sab message.
function validSabMsg() {
  const [a, b] = syncPair(true);
  let msg = null; b.on("lw:mb:sab", (m) => { msg = m; });
  mailbox(a, CFG6, { role: "producer", mode: "shared" });
  return msg;
}
// Fake transport: records handlers, hands out the throwing off.
function fake6(hooks) {
  const t = {
    offCalls: 0, h: new Map(),
    post(type, d) { if (hooks && hooks.post) hooks.post(type, d); },
    on(type, fn) {
      if (hooks && hooks.on) hooks.on(type, fn);
      t.h.set(type, fn);
      return () => { t.offCalls++; throw new Error("off-boom"); };
    },
  };
  return t;
}
const cons6 = (t) => mailbox(t, CFG6, { role: "consumer", mode: "shared", onRecord() {} });
const prod6 = (t) => mailbox(t, CFG6, { role: "producer", mode: "shared" });
// The id is free iff a new lane of the role constructs on the same transport.
function freeAfter6(t, role) {
  t.post = () => {}; t.on = () => () => {};
  assert.ok(role === "consumer" ? cons6(t) : prod6(t), role + ": id freed");
}

test("E6 A1: consumer dispose() throws off once; second is a no-op; id freed", T, () => {
  const t = fake6();
  const c = cons6(t);
  assert.throws(() => c.dispose(), /off-boom/);
  assert.doesNotThrow(() => c.dispose());
  assert.equal(t.offCalls, 1);
  freeAfter6(t, "consumer");
});

test("E6 A2: consumer ctor, post throws: post-boom wins, off once, id freed", T, () => {
  const t = fake6({ post(type) { if (type === "lw:mb:hello") throw new Error("post-boom"); } });
  assert.throws(() => cons6(t), /post-boom/);
  assert.equal(t.offCalls, 1);
  freeAfter6(t, "consumer");
});

test("E6 A3: consumer replay detach throws off-boom; off once; id freed", T, () => {
  const msg = validSabMsg();
  const t = fake6({ on(type, fn) { if (type === "lw:mb:sab") fn(msg); } });
  assert.throws(() => cons6(t), /off-boom/);
  assert.equal(t.offCalls, 1);
  freeAfter6(t, "consumer");
});

test("E6 A4: consumer listener detach throws; attached; dispose does not re-call off", T, () => {
  const msg = validSabMsg();
  const t = fake6();
  const c = cons6(t);
  assert.throws(() => t.h.get("lw:mb:sab")(msg), /off-boom/);
  assert.equal(c.attached, true);
  assert.equal(t.offCalls, 1);
  c.dispose();
  assert.equal(t.offCalls, 1, "off not called a second time");
  freeAfter6(t, "consumer");
});

test("E6 A5: producer ctor, post throws: post-boom wins, off once, id freed", T, () => {
  const t = fake6({ post(type) { if (type === "lw:mb:sab") throw new Error("post-boom"); } });
  assert.throws(() => prod6(t), /post-boom/);
  assert.equal(t.offCalls, 1);
  freeAfter6(t, "producer");
});

test("E6 A6: producer dispose() throws off once; second no-op; push RETIRED; id freed", T, () => {
  const t = fake6();
  const p = prod6(t);
  assert.throws(() => p.dispose(), /off-boom/);
  assert.doesNotThrow(() => p.dispose());
  assert.equal(t.offCalls, 1);
  assert.equal(p.push([1], 0), 4);
  freeAfter6(t, "producer");
});


test("E6 A7: teardown runs before a throwing off(); the listener it leaves is inert", T, () => {
  for (const viaCtor of [true, false]) {
    let meta = null, atOff;
    const t = fake6({ post(type, d) {
      if (type !== "lw:mb:sab") return;
      if (meta !== null) throw new Error("posted after teardown");
      meta = d; if (viaCtor) throw new Error("post-boom");
    } });
    const on6 = t.on;
    t.on = (type, fn) => { const off = on6(type, fn); return () => { atOff = meta.sab; off(); }; };
    if (viaCtor) assert.throws(() => prod6(t), /post-boom/);
    else assert.throws(() => prod6(t).dispose(), /off-boom/);
    assert.equal(atOff, null, "SAB unpinned before off() runs");
    t.h.get("lw:mb:hello")({ id: 0 });   // a retired producer's listener never re-posts
  }
  const t = fake6();
  const c = cons6(t);
  assert.throws(() => c.dispose(), /off-boom/);
  t.h.get("lw:mb:sab")(validSabMsg());
  assert.equal(c.attached, false, "a disposed consumer's leftover listener stays inert");
});
