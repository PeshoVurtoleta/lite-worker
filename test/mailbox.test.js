// test/mailbox.test.js -- unit tests for the mailbox() SPSC lane
// (shared path). Written from the B1 spec, not from the implementation.
// Expected values come from the spec's layout table and from hand arithmetic
// noted next to each case.

import { test } from "node:test";
import { spawnSync } from "node:child_process";
import { pathToFileURL, fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { syncPair, settle, makeTracker, NOOP_CLEANUP } from "./torture/harness.mjs";

const NS = await import("../Worker.js");
const { mailbox, defineWorker } = NS;
const T = { timeout: 30000 };
const ERR = (e) => e instanceof Error && e.message.startsWith("lite-worker: mailbox");

// Records every post() on a transport as { type, data } (data shallow-copied so a
// reused message object cannot rewrite history). Install before mailbox().
function tap(t) {
  const log = [], p0 = t.post.bind(t);
  t.post = (type, data) => {
    log.push({ type, data: data !== null && typeof data === "object" && !ArrayBuffer.isView(data) ? { ...data } : data });
    return p0(type, data);
  };
  return log;
}

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
  // B3a (plan ASSERTION 3 / ASK4a): the nak reason per case; null = ignored, no reply.
  const why = {
    "plain ArrayBuffer": 6, "wrong byteLength": 1, "STATE retired": null, "SLOTS nonzero": 1,
    "SLOT_BYTES nonzero": 1, "CAP mismatch": 2, "W mismatch": 3, "TYPE mismatch": 4,
    "GEN != msg.gen": 5, "mismatched id": null,
  };
  for (const name of Object.keys(cases)) {
    const [a, b] = syncPair(true);
    const got = [];
    const log = tap(b);
    const cons = mailbox(b, cfg, { role: "consumer", mode: "shared", onRecord: (x, o) => got.push(x[o]) });
    a.post("lw:mb:sab", cases[name]);
    const replies = log.filter((e) => e.type === "lw:mb:ack" || e.type === "lw:mb:nak");
    if (why[name] === null) assert.equal(replies.length, 0, name + ": no reply");
    else assert.deepEqual(replies.map((e) => [e.type, e.data]),
      [["lw:mb:nak", { id: 0, gen: cases[name].gen, why: why[name] }]], name + ": exactly one nak");
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

// B3a: mode defaults to "auto" (ASK3a) and "auto" / "transfer" are built, so
// the HEAD cases for missing / auto / transfer mode are gone; "sab" and 1 throw.
test("construction errors", T, () => {
  const [a, b] = syncPair(true);
  const ok = { capacity: 4, words: 1 };
  const P = { role: "producer", mode: "shared" };
  const bad = [
    [{ ...ok, capacity: 0 }, P], [{ ...ok, capacity: 3 }, P],
    [{ ...ok, words: 0 }, P], [{ ...ok, words: 1.5 }, P],
    [{ ...ok, type: "u8" }, P],
    [ok, { mode: "shared" }],
    [ok, { role: "producer", mode: "sab" }],
    [ok, { role: "producer", mode: 1 }],
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
  // B3a: no consumer answers this producer (its offer goes to the other end),
  // so it stays pre-verdict with credit 0 and push is FULL, not OK.
  assert.equal(p2.push([2], 0), 1);
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
  b.post("lw:mb:ack", { id: 0, gen: 1, credit: 0, attached: true });   // B3a: switch to the SAB path
  assert.equal(p.attached, true);
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
    offCalls: 0, onRet: 0, h: new Map(),
    post(type, d) { if (hooks && hooks.post) hooks.post(type, d); },
    on(type, fn) {
      if (hooks && hooks.on) hooks.on(type, fn);
      t.h.set(type, fn);
      t.onRet++;   // B3a: every on() that returned must have its off() run exactly once
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
  assert.equal(t.onRet, 2, "consumer installs sab + rec");
  assert.throws(() => c.dispose(), /off-boom/);
  assert.doesNotThrow(() => c.dispose());
  assert.equal(t.offCalls, 2, "both offs ran, once each (first throw rethrown after all ran)");
  freeAfter6(t, "consumer");
});

test("E6 A2: consumer ctor, post throws: post-boom wins, off once, id freed", T, () => {
  const t = fake6({ post(type) { if (type === "lw:mb:hello") throw new Error("post-boom"); } });
  assert.throws(() => cons6(t), /post-boom/);
  assert.equal(t.onRet, 2);
  assert.equal(t.offCalls, 2, "every installed off ran once; their throws are dropped");
  freeAfter6(t, "consumer");
});

test("E6 A3: consumer replay detach throws off-boom; off once; id freed", T, () => {
  const msg = validSabMsg();
  const t = fake6({ on(type, fn) { if (type === "lw:mb:sab") fn(msg); } });
  assert.throws(() => cons6(t), /off-boom/);
  // B3a: the number of on() calls that returned, not a fixed 1 (the sab replay runs inside on()).
  assert.equal(t.onRet, 1);
  assert.equal(t.offCalls, t.onRet, "each off that exists ran exactly once");
  freeAfter6(t, "consumer");
});

test("E6 A4: consumer listener detach throws; attached; dispose does not re-call off", T, () => {
  const msg = validSabMsg();
  const t = fake6();
  const c = cons6(t);
  assert.throws(() => t.h.get("lw:mb:sab")(msg), /off-boom/);
  assert.equal(c.attached, true);
  assert.equal(t.offCalls, 1);
  // B3a: dispose now runs the remaining (rec) off, which throws; sab off is not re-called.
  assert.throws(() => c.dispose(), /off-boom/);
  assert.equal(t.offCalls, 2, "sab off not called a second time; rec off once");
  assert.doesNotThrow(() => c.dispose());
  assert.equal(t.offCalls, 2);
  freeAfter6(t, "consumer");
});

test("E6 A5: producer ctor, post throws: post-boom wins, off once, id freed", T, () => {
  const t = fake6({ post(type) { if (type === "lw:mb:sab") throw new Error("post-boom"); } });
  assert.throws(() => prod6(t), /post-boom/);
  assert.equal(t.onRet, 3, "producer installs hello + ack + nak");
  assert.equal(t.offCalls, 3);
  freeAfter6(t, "producer");
});

test("E6 A6: producer dispose() throws off once; second no-op; push RETIRED; id freed", T, () => {
  const t = fake6();
  const p = prod6(t);
  assert.equal(t.onRet, 3);
  assert.throws(() => p.dispose(), /off-boom/);
  assert.doesNotThrow(() => p.dispose());
  assert.equal(t.offCalls, 3);
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

// ===========================================================================
// B3a: fallback ring, credit, ack / nak, FBN switch, verdict latch
// (from .handoff/b3/plan.md ASSERTIONS A1-A6 + AD4; each fails on the a3e6a3c
// build: HEAD throws on mode auto / transfer and never posts ack / nak / rec)
// ===========================================================================

// Sync pair whose deliveries of the listed message types are queued until
// release(type). Everything else is delivered synchronously, like syncPair.
function heldPair(holdTypes) {
  const hold = new Set(holdTypes), q = [];
  const mk = () => ({
    _t: new Map(), other: null,
    on(type, fn) { let s = this._t.get(type); if (!s) { s = new Set(); this._t.set(type, s); } s.add(fn); return () => s.delete(fn); },
    post(type, data) {
      if (hold.has(type)) { q.push({ type, to: this.other, data }); return; }
      const s = this.other._t.get(type); if (s) s.forEach((f) => f(data));
    },
  });
  const a = mk(), b = mk(); a.other = b; b.other = a;
  return {
    a, b,
    pending: (type) => q.filter((e) => e.type === type).map((e) => e.data),
    release(type) {
      hold.delete(type);
      const mine = [];
      for (let i = q.length - 1; i >= 0; i--) if (q[i].type === type) mine.unshift(q.splice(i, 1)[0]);
      for (const e of mine) { const s = e.to._t.get(type); if (s) s.forEach((f) => f(e.data)); }
    },
  };
}
const isReply = (e) => e.type === "lw:mb:ack" || e.type === "lw:mb:nak";
const sumMbListeners = (t) => { let n = 0; for (const [k, v] of t._t) if (k.startsWith("lw:mb:")) n += v.size; return n; };

test("B3a A1: transfer lane, credit window of CAP, one ack after drain", T, () => {
  const CAP = 4, W = 2, cfg = { capacity: CAP, words: W };
  const [a, b] = syncPair(true);
  const aLog = tap(a), bLog = tap(b);
  const prod = mailbox(a, cfg, { role: "producer", mode: "transfer" });
  assert.equal(prod.push([1, 10], 0), 1, "no consumer yet: credit 0 -> FULL");
  const got = [];
  const cons = mailbox(b, cfg, { role: "consumer", mode: "transfer", onRecord: (x, o) => got.push([x[o], x[o + 1]]) });
  for (let i = 1; i <= CAP; i++) assert.equal(prod.push([i, i * 10], 0), 0, "push " + i);
  assert.equal(prod.push([99, 99], 0), 1, "5th push: credit exhausted");
  // rec messages: fields exactly {id, v}, v a lane-type typed array of length W
  const recs = aLog.filter((e) => e.type === "lw:mb:rec");
  assert.equal(recs.length, CAP);
  recs.forEach((e, i) => {
    assert.deepEqual(Object.keys(e.data).sort(), ["id", "v"]);
    assert.equal(e.data.id, 0);
    assert.ok(e.data.v instanceof Int32Array);
    assert.deepEqual(Array.from(e.data.v), [i + 1, (i + 1) * 10]);
  });
  assert.equal(bLog.filter((e) => e.type === "lw:mb:ack").length, 0, "no ack before the drain");
  assert.equal(cons.drain(), CAP);
  assert.deepEqual(got, [[1, 10], [2, 20], [3, 30], [4, 40]]);
  const acks = bLog.filter((e) => e.type === "lw:mb:ack");
  assert.deepEqual(acks.map((e) => e.data), [{ id: 0, gen: 1, credit: CAP, attached: false }]);
  // AD4: a transfer-mode consumer naks the (sab-less) offer with why 6
  assert.deepEqual(bLog.filter((e) => e.type === "lw:mb:nak").map((e) => e.data), [{ id: 0, gen: 1, why: 6 }]);
  for (let i = 5; i <= 8; i++) assert.equal(prod.push([i, i * 10], 0), 0, "credit returned: push " + i);
  assert.equal(cons.drain(), CAP);
  assert.deepEqual(got.slice(4), [[5, 50], [6, 60], [7, 70], [8, 80]]);
  assert.equal(prod.mode, "transfer");
  assert.equal(prod.attached, false);
  assert.equal(cons.attached, false);
});

test("B3a A2: FBN keeps pre-verdict fallback records ahead of SAB records", T, () => {
  const hp = heldPair(["lw:mb:rec", "lw:mb:ack"]);
  let sab = null; hp.b.on("lw:mb:sab", (m) => { if (m.sab) sab = m.sab; });
  const cfg = { capacity: 8, words: 1 };
  const prod = mailbox(hp.a, cfg, { role: "producer", mode: "shared" });
  const got = [];
  const cons = mailbox(hp.b, cfg, { role: "consumer", mode: "shared", onRecord: (x, o) => got.push(x[o]) });
  for (let i = 1; i <= 3; i++) assert.equal(prod.push([i], 0), 0, "pre-verdict push " + i);   // fallback, held
  assert.equal(hp.pending("lw:mb:ack").length, 1, "the attach verdict is still in flight");
  hp.release("lw:mb:ack");                                                                    // verdict: attached
  for (let i = 4; i <= 5; i++) assert.equal(prod.push([i], 0), 0, "SAB push " + i);
  assert.ok(sab, "sab captured");
  const i32 = new Int32Array(sab);
  assert.equal(Atomics.load(i32, FBN), 3, "FBN = fallback records accepted before the switch");
  assert.equal(Atomics.load(i32, TAIL) - Atomics.load(i32, HEAD), 2, "2 records on the SAB");
  assert.equal(cons.drain(), 0, "SAB must not be read while 3 earlier records are held back");
  assert.deepEqual(got, []);
  hp.release("lw:mb:rec");
  assert.equal(cons.drain(), 5);
  assert.deepEqual(got, [1, 2, 3, 4, 5]);
  assert.equal(cons.drain(), 0);
  assert.equal(prod.attached, true);
  assert.equal(cons.attached, true);
});

test("B3a A3: offers get exactly one ack or nak with the reason of the first failing field", T, () => {
  const cfg = { capacity: 4, words: 2 };
  const size = 384 + r128(4 * 2 * 4) + 128;   // layout table, i32
  const mk = (mut, sz = size, Ctor = SharedArrayBuffer) => {
    const s = new Ctor(sz), v = new Int32Array(s);
    v[GEN] = 1; v[CAPW] = 4; v[WW] = 2; v[TYPEW] = 0;
    mut(v); return s;
  };
  const none = () => {};
  // [name, message, expected why]; order of precedence 6, 1, 2, 3, 4, then 5
  const cases = [
    ["plain ArrayBuffer", { id: 0, gen: 1, sab: mk(none, size, ArrayBuffer) }, 6],
    ["byteLength + 128", { id: 0, gen: 1, sab: mk(none, size + 128) }, 1],
    ["SLOTS != 0", { id: 0, gen: 1, sab: mk((v) => { v[SLOTS] = 1; }) }, 1],
    ["SLOT_BYTES != 0", { id: 0, gen: 1, sab: mk((v) => { v[SLOT_BYTES] = 8; }) }, 1],
    ["CAP 8", { id: 0, gen: 1, sab: mk((v) => { v[CAPW] = 8; }) }, 2],
    ["W 3", { id: 0, gen: 1, sab: mk((v) => { v[WW] = 3; }) }, 3],
    ["TYPE 1", { id: 0, gen: 1, sab: mk((v) => { v[TYPEW] = 1; }) }, 4],
    ["gen + 1", { id: 0, gen: 2, sab: mk(none) }, 5],
    ["plain AB beats every other failure", { id: 0, gen: 2, sab: mk((v) => { v[CAPW] = 8; }, size + 128, ArrayBuffer) }, 6],
    ["byteLength beats CAP", { id: 0, gen: 1, sab: mk((v) => { v[CAPW] = 8; }, size + 128) }, 1],
    ["CAP beats W", { id: 0, gen: 1, sab: mk((v) => { v[CAPW] = 8; v[WW] = 3; }) }, 2],
    ["W beats TYPE", { id: 0, gen: 1, sab: mk((v) => { v[WW] = 3; v[TYPEW] = 1; }) }, 3],
    ["TYPE beats gen", { id: 0, gen: 2, sab: mk((v) => { v[TYPEW] = 1; }) }, 4],
  ];
  for (const [name, msg, why] of cases) {   // one consumer per case: the verdict latches
    const [a, b] = syncPair(true), log = tap(b);
    const cons = mailbox(b, cfg, { role: "consumer", mode: "shared", onRecord() {} });
    const base = log.length;
    a.post("lw:mb:sab", msg);
    const replies = log.slice(base).filter(isReply);
    assert.deepEqual(replies.map((e) => [e.type, e.data]), [["lw:mb:nak", { id: 0, gen: msg.gen, why }]], name);
    assert.equal(cons.attached, false, name);
    a.post("lw:mb:sab", { id: 0, gen: 1, sab: mk(none) });   // latch: a later (valid) offer is ignored
    assert.equal(log.slice(base).filter(isReply).length, 1, name + ": no reply after the verdict");
    assert.equal(cons.attached, false, name + ": latched");
  }
  // STATE 1 is ignored with no reply and does not use up the verdict; a valid offer then acks once.
  const [a, b] = syncPair(true), log = tap(b);
  const cons = mailbox(b, cfg, { role: "consumer", mode: "shared", onRecord() {} });
  const base = log.length;
  a.post("lw:mb:sab", { id: 0, gen: 1, sab: mk((v) => { v[STATE] = 1; }) });
  assert.equal(log.slice(base).filter(isReply).length, 0, "STATE 1: no reply");
  assert.equal(cons.attached, false);
  a.post("lw:mb:sab", { id: 0, gen: 1, sab: mk(none) });
  assert.deepEqual(log.slice(base).filter(isReply).map((e) => [e.type, e.data]),
    [["lw:mb:ack", { id: 0, gen: 1, credit: 0, attached: true }]]);
  assert.equal(cons.attached, true);
  a.post("lw:mb:sab", { id: 0, gen: 1, sab: mk((v) => { v[CAPW] = 8; }) });
  assert.equal(log.slice(base).filter(isReply).length, 1, "no second verdict");
  assert.equal(cons.attached, true);
});

// The handshake is OK iff the producer saw a nak and nothing after it was FULL (1).
function handshakeOk(tr) {
  return tr.nakSeen === true && tr.outcomes.slice(tr.nakIdx).indexOf(1) < 0;
}
// Producer cap 4, consumer cap 8: the consumer naks the offer with why 2. The nak is held,
// so two records are accepted pre-verdict (window opened by the consumer's hello).
function nakRun(mode) {
  const hp = heldPair(["lw:mb:nak"]);
  const prod = mailbox(hp.a, { capacity: 4, words: 1 }, { role: "producer", mode });
  const got = [];
  const cons = mailbox(hp.b, { capacity: 8, words: 1 }, { role: "consumer", mode, onRecord: (x, o) => got.push(x[o]) });
  const tr = { outcomes: [], nakSeen: false, nakIdx: -1 };
  assert.deepEqual(hp.pending("lw:mb:nak"), [{ id: 0, gen: 1, why: 2 }]);
  tr.outcomes.push(prod.push([1], 0), prod.push([2], 0));
  hp.release("lw:mb:nak");
  tr.nakSeen = true; tr.nakIdx = tr.outcomes.length;
  return { hp, prod, cons, got, tr };
}

test("B3a A4: a nak ends the SAB attempt: shared -> REJECTED, auto -> transfer, never FULL forever", T, () => {
  // shared: the 2 pre-verdict records still arrive; every later push is REJECTED (5)
  const s = nakRun("shared");
  const after = [s.prod.push([5], 0), s.prod.push([5], 0), s.prod.push([5], 0)];
  assert.deepEqual(after, [5, 5, 5]);
  const accepted = s.tr.outcomes.filter((o) => o === 0).length;
  assert.equal(accepted, 2);
  assert.equal(s.cons.drain(), 2, "delivered === accepted");
  assert.deepEqual(s.got, [1, 2]);
  assert.equal(s.prod.mode, "shared");
  assert.equal(s.prod.attached, false);
  // auto: settles on "transfer" and keeps flowing through the credit window
  const u = nakRun("auto");
  assert.equal(u.prod.mode, "transfer");
  assert.equal(u.cons.mode, "transfer");
  assert.equal(u.prod.attached, false);
  assert.equal(u.cons.drain(), 2);   // ASSUMED: pre-verdict records drained first so credit is back to CAP
  const CAP = 4, seq = [];
  for (let c = 0; c < 3; c++) {
    for (let k = 0; k < CAP; k++) { const v = 100 + c * CAP + k; seq.push(v); u.tr.outcomes.push(u.prod.push([v], 0)); }
    assert.equal(u.cons.drain(), CAP);
  }
  const post = u.tr.outcomes.slice(u.tr.nakIdx);
  assert.equal(post.length, 3 * CAP);
  assert.ok(post.every((o) => o === 0), "0 FULL and never REJECTED after the nak: " + post);
  assert.deepEqual(u.got, [1, 2].concat(seq));
  assert.equal(handshakeOk(u.tr), true);
  assert.equal(handshakeOk(s.tr), true);
  // C11 control: the old trace (CAP accepted, then FULL, no nak ever seen) must fail the predicate.
  const hp = heldPair(["lw:mb:nak"]);   // never released: the producer never sees the nak
  const prod = mailbox(hp.a, { capacity: 4, words: 1 }, { role: "producer", mode: "auto" });
  mailbox(hp.b, { capacity: 8, words: 1 }, { role: "consumer", mode: "auto", onRecord() {} });
  const old = { outcomes: [], nakSeen: false, nakIdx: -1 };
  for (let i = 0; i < 5; i++) old.outcomes.push(prod.push([i], 0));
  assert.deepEqual(old.outcomes, [0, 0, 0, 0, 1]);
  assert.equal(handshakeOk(old), false, "no nak: handshakeOk rejects the old trace");
  assert.equal(handshakeOk({ outcomes: [0, 0, 0, 0, 1], nakSeen: false, nakIdx: -1 }), false);
  assert.equal(handshakeOk({ outcomes: [0, 1, 0], nakSeen: true, nakIdx: 1 }), false, "FULL after the nak also fails");
});

test("B3a A5: corrupt TAIL kills the lane; a leftover listener of a throwing off() stays inert", T, () => {
  const msg = validSabMsg();
  const posts = [];
  const t = fake6({ post(type, d) { posts.push({ type, data: d }); } });
  const c = cons6(t);
  assert.throws(() => t.h.get("lw:mb:sab")(msg), /off-boom/);   // attach, then the sab off() throws
  assert.equal(c.attached, true);
  const i32 = new Int32Array(msg.sab);
  Atomics.store(i32, TAIL, (Atomics.load(i32, HEAD) + CFG6.capacity + 1) | 0);
  assert.equal(c.drain(), 0);
  assert.equal(c.attached, false);
  const before = posts.filter(isReply).length;
  const fresh = validSabMsg();
  assert.doesNotThrow(() => t.h.get("lw:mb:sab")(fresh));
  assert.equal(c.attached, false, "the leftover listener must not re-attach");
  assert.equal(c.drain(), 0);
  assert.equal(posts.filter(isReply).length, before, "0 acks / naks for the fresh offer");
  assert.equal(t.offCalls, 1, "the sab off() is not called again");
});

test("B3a A6: transfer / auto-attached / auto-naked pairs leave no listener or registry entry", T, async (t) => {
  const { tracker } = makeTracker();
  const CYCLES = 64, kinds = ["transfer", "auto-attached", "auto-naked"];
  function cycle(kind) {
    const [a, b] = syncPair(true);
    const mode = kind === "transfer" ? "transfer" : "auto";
    const cfgP = { capacity: 4, words: 1 }, cfgC = kind === "auto-naked" ? { capacity: 8, words: 1 } : cfgP;
    const prod = mailbox(a, cfgP, { role: "producer", mode });
    let n = 0;
    const cons = mailbox(b, cfgC, { role: "consumer", mode, onRecord() { n++; } });
    for (let i = 0; i < 3; i++) assert.equal(prod.push([i], 0), 0, kind);
    assert.equal(cons.drain(), 3, kind);
    assert.equal(n, 3, kind);
    if (kind === "auto-attached") { assert.equal(prod.mode, "shared"); assert.equal(cons.attached, true); }
    if (kind === "auto-naked") { assert.equal(prod.mode, "transfer"); assert.equal(cons.attached, false); }
    prod.dispose(); cons.dispose();
    assert.equal(sumMbListeners(a) + sumMbListeners(b), 0, kind + ": lw:mb:* listeners left");
    for (const t of [a, b]) assert.equal(t.__lwMb.producer.size + t.__lwMb.consumer.size, 0, kind + ": registry not empty");
    tracker.track(a, NOOP_CLEANUP, "pair-" + kind); tracker.track(b, NOOP_CLEANUP, "pair-" + kind);
    tracker.track(prod, NOOP_CLEANUP, "prod-" + kind); tracker.track(cons, NOOP_CLEANUP, "cons-" + kind);
  }
  for (let i = 0; i < CYCLES; i++) for (const k of kinds) cycle(k);
  // Keep-alive leg: a transport whose off() throws stays referenced, so its leftover listeners
  // are alive; the lane's fallback ring (tracked from inside onRecord) must still be collectable.
  const keep = [];
  for (let i = 0; i < 8; i++) {
    const ft = fake6(); keep.push(ft);
    let tracked = false;
    const c = mailbox(ft, CFG6, { role: "consumer", mode: "shared", onRecord(x) {
      if (!tracked) { tracked = true; tracker.track(x, NOOP_CLEANUP, "ring-" + i); }
    } });
    ft.h.get("lw:mb:rec")({ id: 0, v: new Int32Array([i]) });
    assert.equal(c.drain(), 1);
    assert.throws(() => c.dispose(), /off-boom/);
    assert.equal(c.drain(), 0);
  }
  if (typeof globalThis.gc === "function") {
    await settle(); await settle();
    assert.equal(tracker.size(), 0, "tracked pairs / lanes / rings retained");
  } else {
    t.diagnostic("B3a A6: GC leg SKIPPED (needs node --expose-gc); the B3a A7n child-process test covers ring retention");
  }
  assert.equal(keep.length, 8, "the throwing-off transports stayed referenced through the GC leg");
});

// ===========================================================================
// B3a A7: guards the round-1 mutants slipped past (one scenario per test; each
// name says the guard it pins). Expected values are derived from the D4
// protocol text and AD4-AD6 in .handoff/b3/plan.md.
// ===========================================================================

// Lone consumer on b; a.post("lw:mb:rec", ...) spoofs the producer's records.
function lone(cfg, mode) {
  const [a, b] = syncPair(true), got = [];
  const W = cfg.words;
  const cons = mailbox(b, cfg, { role: "consumer", mode: mode || "transfer",
    onRecord: (x, o) => { const r = []; for (let k = 0; k < W; k++) r.push(x[o + k]); got.push(r); } });
  return { a, b, cons, got };
}
// Lone producer on a (mode as given); b.post spoofs the consumer's replies.
function loneProd(mode) {
  const [a, b] = syncPair(true), log = tap(a);
  return { a, b, log, prod: mailbox(a, CFG6, { role: "producer", mode }) };
}
const hello = (r) => r.b.post("lw:mb:hello", { id: 0 });
const nRec = (r) => r.log.filter((e) => e.type === "lw:mb:rec").length;

test("B3a A7a: a rec after the SAB latch (sw) kills the lane (drain 0 forever, detached)", T, () => {
  const [a, b] = syncPair(true), got = [];
  const prod = mailbox(a, CFG6, { role: "producer", mode: "shared" });
  const cons = mailbox(b, CFG6, { role: "consumer", mode: "shared", onRecord: (x, o) => got.push(x[o]) });
  assert.equal(prod.push([1], 0), 0);
  assert.equal(cons.drain(), 1, "SAB read: the switch is latched");
  a.post("lw:mb:rec", { id: 0, v: new Int32Array([7]) });
  assert.equal(prod.push([2], 0), 0);
  assert.equal(cons.drain(), 0);
  assert.equal(cons.attached, false);
  assert.equal(cons.drain(), 0);
  assert.deepEqual(got, [1]);
});

test("B3a A7b: the (CAP+1)th rec without a drain kills the lane (fallback ring overflow)", T, () => {
  const ok = lone(CFG6);   // control: exactly CAP records are fine
  for (let i = 1; i <= 4; i++) ok.a.post("lw:mb:rec", { id: 0, v: new Int32Array([i]) });
  assert.equal(ok.cons.drain(), 4);
  assert.deepEqual(ok.got, [[1], [2], [3], [4]]);
  const r = lone(CFG6);
  for (let i = 1; i <= 5; i++) r.a.post("lw:mb:rec", { id: 0, v: new Int32Array([i]) });
  assert.equal(r.cons.drain(), 0);
  assert.equal(r.cons.attached, false);
  r.a.post("lw:mb:rec", { id: 0, v: new Int32Array([6]) });
  assert.equal(r.cons.drain(), 0, "dead for good");
  assert.deepEqual(r.got, []);
});

test("B3a A7c: a rec of the wrong container type kills the lane", T, () => {
  const bad = [
    [{ capacity: 4, words: 1 }, new Float64Array([5])],
    [{ capacity: 4, words: 1 }, [5]],
    [{ capacity: 4, words: 1 }, new Uint32Array([5])],
    [{ capacity: 4, words: 1, type: "f64" }, new Int32Array([5])],
    [{ capacity: 4, words: 1, type: "f64" }, [5]],
  ];
  for (const [cfg, v] of bad) {
    const r = lone(cfg);
    r.a.post("lw:mb:rec", { id: 0, v });
    assert.equal(r.cons.drain(), 0, cfg.type + " / " + Object.prototype.toString.call(v));
    assert.deepEqual(r.got, []);
    assert.equal(r.cons.attached, false);
  }
  const f = lone({ capacity: 4, words: 1, type: "f64" });   // control: the right type is accepted
  f.a.post("lw:mb:rec", { id: 0, v: new Float64Array([0.5]) });
  assert.equal(f.cons.drain(), 1);
  assert.deepEqual(f.got, [[0.5]]);
});

test("B3a A7d: a rec whose length is not W kills the lane", T, () => {
  for (const len of [0, 1, 3]) {
    const r = lone({ capacity: 4, words: 2 });
    r.a.post("lw:mb:rec", { id: 0, v: new Int32Array(len) });
    assert.equal(r.cons.drain(), 0, "length " + len);
    assert.deepEqual(r.got, []);
  }
  const r = lone({ capacity: 4, words: 2 });   // control: length W
  r.a.post("lw:mb:rec", { id: 0, v: new Int32Array([3, 4]) });
  assert.equal(r.cons.drain(), 1);
  assert.deepEqual(r.got, [[3, 4]]);
});

test("B3a A7e: an ack beyond cap - credit is ignored (window never exceeds CAP)", T, () => {
  const ack = (r, credit) => r.b.post("lw:mb:ack", { id: 0, gen: 1, credit, attached: false });
  const fill = (r) => { const o = []; for (let i = 0; i < 6; i++) o.push(r.prod.push([i], 0)); return o; };
  const p = loneProd("transfer"); hello(p);
  assert.deepEqual(fill(p), [0, 0, 0, 0, 1, 1], "window of CAP = 4");
  ack(p, 5);
  assert.equal(p.prod.push([9], 0), 1, "credit 5 > cap - 0: ignored");
  ack(p, 4);   // exactly cap - credit: accepted
  assert.deepEqual(fill(p), [0, 0, 0, 0, 1, 1]);
  const q = loneProd("transfer"); hello(q);   // credit is full (4): even 1 more would exceed CAP
  ack(q, 1);
  assert.deepEqual(fill(q), [0, 0, 0, 0, 1, 1], "credit 1 on a full window: ignored");
  // ASSUMED (fail closed, not stated by D4): a non-integer / negative / non-number credit is ignored too.
  for (const k of [NaN, 1.5, -1, "2", Infinity, 1e10]) {
    const r = loneProd("transfer"); hello(r);
    assert.deepEqual(fill(r).slice(0, 4), [0, 0, 0, 0]);
    ack(r, k);
    assert.equal(r.prod.push([9], 0), 1, "credit " + String(k) + " ignored");
  }
});

test("B3a A7f: ack / nak with gen 2 have no effect (no attach, no open, no credit, no verdict)", T, () => {
  const a1 = loneProd("auto");
  a1.b.post("lw:mb:ack", { id: 0, gen: 2, credit: 0, attached: true });
  assert.equal(a1.prod.attached, false);
  assert.equal(a1.prod.mode, "auto");
  assert.equal(a1.prod.push([1], 0), 1, "a gen-2 ack does not open the window");
  hello(a1);
  assert.equal(a1.prod.push([1], 0), 0);
  assert.equal(a1.prod.push([2], 0), 0);
  a1.b.post("lw:mb:ack", { id: 0, gen: 2, credit: 2, attached: false });
  const o = [a1.prod.push([3], 0), a1.prod.push([4], 0), a1.prod.push([5], 0)];
  assert.deepEqual(o, [0, 0, 1], "credit 2 of the gen-2 ack was not added");
  assert.equal(a1.prod.attached, false);
  assert.equal(nRec(a1), 4);
  const n1 = loneProd("auto");
  n1.b.post("lw:mb:nak", { id: 0, gen: 2, why: 2 });
  assert.equal(n1.prod.mode, "auto", "a gen-2 nak: no switch to transfer");
  assert.equal(n1.prod.push([1], 0), 1, "a gen-2 nak does not open the window");
  const n2 = loneProd("shared");
  n2.b.post("lw:mb:nak", { id: 0, gen: 2, why: 2 });
  hello(n2);
  assert.equal(n2.prod.push([1], 0), 0, "a gen-2 nak does not reject a shared lane");
  assert.equal(n2.prod.mode, "shared");
});

test("B3a A7g: a nak after the attached ack is ignored (verdict latch)", T, () => {
  const [a, b] = syncPair(true), alog = tap(a), got = [];
  const prod = mailbox(a, CFG6, { role: "producer", mode: "auto" });
  const cons = mailbox(b, CFG6, { role: "consumer", mode: "auto", onRecord: (x, o) => got.push(x[o]) });
  assert.equal(prod.attached, true);
  b.post("lw:mb:nak", { id: 0, gen: 1, why: 2 });
  assert.equal(prod.attached, true);
  assert.equal(prod.mode, "shared");
  assert.equal(prod.push([5], 0), 0);
  assert.equal(alog.filter((e) => e.type === "lw:mb:rec").length, 0, "the record went through the SAB");
  assert.equal(cons.drain(), 1);
  assert.deepEqual(got, [5]);
  assert.equal(cons.attached, true);
});

test("B3a A7h: FBN below the delivered fallback count kills the lane; FBN equal reads the SAB", T, () => {
  // heldPair holds the acks, so the producer stays pre-verdict while the consumer attaches.
  function run(fbn) {
    const hp = heldPair(["lw:mb:ack"]), got = [];
    let sab = null; hp.b.on("lw:mb:sab", (m) => { if (m.sab) sab = m.sab; });
    const prod = mailbox(hp.a, CFG6, { role: "producer", mode: "shared" });
    const cons = mailbox(hp.b, CFG6, { role: "consumer", mode: "shared", onRecord: (x, o) => got.push(x[o]) });
    assert.equal(prod.push([1], 0), 0); assert.equal(prod.push([2], 0), 0);
    assert.equal(cons.drain(), 2, "two fallback records delivered");
    assert.equal(cons.attached, true);
    const i32 = new Int32Array(sab);
    i32[96] = 77;                       // payload slot 0 (byte 384)
    Atomics.store(i32, FBN, fbn);       // forged: what the producer claims it sent as fallback
    Atomics.store(i32, TAIL, 1);
    return { cons, got };
  }
  const dead = run(1);                  // (fbHead - FBN) = 1 > 0
  assert.equal(dead.cons.drain(), 0);
  assert.equal(dead.cons.attached, false);
  assert.equal(dead.cons.drain(), 0);
  assert.deepEqual(dead.got, [1, 2]);
  const ok = run(2);                    // control: equal -> the SAB record is delivered
  assert.equal(ok.cons.drain(), 1);
  assert.deepEqual(ok.got, [1, 2, 77]);
  const wait = run(3);                  // fewer delivered than FBN: wait, not dead
  assert.equal(wait.cons.drain(), 0);
  assert.equal(wait.cons.attached, true);
  assert.deepEqual(wait.got, [1, 2]);
});

test("B3a A7i: dispose / kill inside onRecord during a FALLBACK drain ends it (AD6 B1)", T, () => {
  for (const how of ["dispose", "kill"]) {
    const [a, b] = syncPair(true);
    const prod = mailbox(a, CFG6, { role: "producer", mode: "transfer" });
    const seen = []; let cons;
    cons = mailbox(b, CFG6, { role: "consumer", mode: "transfer", onRecord(x, o) {
      seen.push(x === null ? "null@" + o : x[o]);
      if (seen.length > 5) throw new Error("runaway drain");   // a broken engine would loop forever
      if (seen.length === 1) { if (how === "dispose") cons.dispose(); else a.post("lw:mb:rec", { id: 0, v: [1] }); }
    } });
    for (let i = 1; i <= 3; i++) assert.equal(prod.push([i], 0), 0);
    assert.equal(cons.drain(), 1, how + ": count so far");
    assert.equal(cons.drain(), 0, how);
    assert.equal(cons.drain(), 0, how);
    assert.deepEqual(seen, [1], how + ": never called again, never with a null view");
  }
});

test("B3a A7j: a throwing ack post() in drain re-posts the owed credit on the next drain (AD6 B2)", T, () => {
  const [a, b] = syncPair(true);
  const prod = mailbox(a, CFG6, { role: "producer", mode: "transfer" });
  const p0 = b.post.bind(b), acks = []; let boom = 1;
  b.post = (type, d) => {
    if (type === "lw:mb:ack" && boom-- > 0) throw new Error("ack-boom");
    if (type === "lw:mb:ack") acks.push({ ...d });
    return p0(type, d);
  };
  let n = 0;
  const cons = mailbox(b, CFG6, { role: "consumer", mode: "transfer", onRecord() { n++; } });
  for (let i = 0; i < 4; i++) assert.equal(prod.push([i], 0), 0);
  assert.equal(prod.push([9], 0), 1);
  assert.throws(() => cons.drain(), /ack-boom/);
  assert.equal(n, 4, "all four delivered before the ack threw");
  assert.equal(prod.push([9], 0), 1, "no credit yet");
  assert.equal(cons.drain(), 0);
  assert.deepEqual(acks, [{ id: 0, gen: 1, credit: 4, attached: false }], "the owed credit was posted");
  for (let i = 0; i < 4; i++) assert.equal(prod.push([i], 0), 0, "accepted again " + i);
  assert.equal(prod.push([9], 0), 1);
  assert.equal(cons.drain(), 4);
});

test("B3a A7k: a nak retires the producer's SAB (STATE 1) and drops meta.sab", T, () => {
  const [a, b] = syncPair(true);
  const cap = { msg: null };
  b.on("lw:mb:sab", (m) => { if (cap.msg === null) cap.msg = m; });
  const prod = mailbox(a, { capacity: 4, words: 1 }, { role: "producer", mode: "shared" });
  assert.ok(cap.msg.sab instanceof SharedArrayBuffer);
  const i32 = new Int32Array(cap.msg.sab);
  assert.equal(Atomics.load(i32, STATE), 0, "live before the verdict");
  const cons = mailbox(b, { capacity: 8, words: 1 }, { role: "consumer", mode: "shared", onRecord() {} });
  assert.equal(Atomics.load(i32, STATE), 1, "nak: STATE retired");
  assert.equal(cap.msg.sab, null, "the posted offer object no longer pins the SAB");
  assert.equal(prod.push([1], 0), 5);
  assert.equal(cons.attached, false);
});

test("B3a A7l: a hello after the verdict makes the producer post no new lw:mb:sab", T, () => {
  for (const cCap of [4, 8]) {   // 4: attached verdict; 8: nak verdict (CAP mismatch)
    const [a, b] = syncPair(true), alog = tap(a);
    mailbox(a, CFG6, { role: "producer", mode: "shared" });
    mailbox(b, { capacity: cCap, words: 1 }, { role: "consumer", mode: "shared", onRecord() {} });
    const sabs = () => alog.filter((e) => e.type === "lw:mb:sab").length;
    assert.equal(sabs(), 2, "cap " + cCap + ": ctor offer + the re-offer for the consumer's first hello");
    b.post("lw:mb:hello", { id: 0 });
    b.post("lw:mb:hello", { id: 0 });
    assert.equal(sabs(), 2, "cap " + cCap + ": no offer after the verdict");
  }
});

test("B3a A7m: the credit ack of an attached consumer says attached: true", T, () => {
  const hp = heldPair(["lw:mb:sab"]), blog = tap(hp.b), got = [];
  const prod = mailbox(hp.a, CFG6, { role: "producer", mode: "shared" });
  const cons = mailbox(hp.b, CFG6, { role: "consumer", mode: "shared", onRecord: (x, o) => got.push(x[o]) });
  assert.equal(prod.push([1], 0), 0); assert.equal(prod.push([2], 0), 0);
  assert.equal(cons.drain(), 2);   // not attached yet: the offers are still held
  assert.equal(prod.push([3], 0), 0);
  hp.release("lw:mb:sab");         // verdict: attached (the second offer is ignored)
  assert.equal(cons.attached, true);
  assert.equal(prod.attached, true);
  assert.equal(cons.drain(), 1);   // one fallback record delivered after the attach
  assert.deepEqual(blog.filter((e) => e.type === "lw:mb:ack").map((e) => e.data), [
    { id: 0, gen: 1, credit: 2, attached: false },
    { id: 0, gen: 1, credit: 0, attached: true },
    { id: 0, gen: 1, credit: 1, attached: true },
  ]);
  assert.equal(prod.push([4], 0), 0);
  assert.equal(cons.drain(), 1);
  assert.deepEqual(got, [1, 2, 3, 4]);
});

// o: credit is spent only after post() returned
test("B3a A7o: a rec post() that throws does not consume credit", T, () => {
  const [a, b] = syncPair(true);
  const prod = mailbox(a, CFG6, { role: "producer", mode: "transfer" });
  const got = [];
  const cons = mailbox(b, CFG6, { role: "consumer", mode: "transfer", onRecord: (x, o) => got.push(x[o]) });
  const p0 = a.post.bind(a); let boom = 1;
  a.post = (type, d) => { if (type === "lw:mb:rec" && boom-- > 0) throw new Error("rec-boom"); return p0(type, d); };
  assert.throws(() => prod.push([0], 0), /rec-boom/);
  for (let i = 1; i <= 4; i++) assert.equal(prod.push([i], 0), 0, "push " + i);
  assert.equal(prod.push([9], 0), 1);
  assert.equal(cons.drain(), 4);
  assert.deepEqual(got, [1, 2, 3, 4], "the thrown record was not accepted");
});

test("B3a A7p (AD5): after ANY nak the consumer reports transfer and still delivers the fallback", T, () => {
  const bad = [["shared", "transfer"], ["auto", "transfer"]];
  for (const [cm, pm] of bad) {   // transfer producer: sab null -> why 6
    const [a, b] = syncPair(true), blog = tap(b), got = [];
    const prod = mailbox(a, CFG6, { role: "producer", mode: pm });
    const cons = mailbox(b, CFG6, { role: "consumer", mode: cm, onRecord: (x, o) => got.push(x[o]) });
    assert.deepEqual(blog.filter((e) => e.type === "lw:mb:nak").map((e) => e.data), [{ id: 0, gen: 1, why: 6 }], cm);
    assert.equal(cons.mode, "transfer", cm + " consumer after the nak");
    assert.equal(cons.attached, false);
    assert.equal(prod.push([1], 0), 0); assert.equal(prod.push([2], 0), 0);
    assert.equal(cons.drain(), 2);
    assert.deepEqual(got, [1, 2], cm + ": lossless");
  }
  // shared <-> shared with a CAP mismatch (why 2): consumer -> transfer, producer stays shared (REJECTED)
  const [a, b] = syncPair(true);
  const prod = mailbox(a, CFG6, { role: "producer", mode: "shared" });
  const cons = mailbox(b, { capacity: 8, words: 1 }, { role: "consumer", mode: "shared", onRecord() {} });
  assert.equal(cons.mode, "transfer");
  assert.equal(prod.mode, "shared");
  assert.equal(prod.push([1], 0), 5);
});

// n: the ring is released on dispose. Only a GC probe can see it, and plain `npm test` has no
// --expose-gc, so the probe runs in a child node that has it. Control: an undisposed lane keeps it.
test("B3a A7n: dispose() drops the fallback ring (kept-alive throwing-off transport)", T, () => {
  const src = [
    'const { mailbox } = await import(process.argv[1]);',
    'const keep = [];',
    'async function probe(dispose) {',
    '  const h = new Map();',
    '  const t = { post() {}, on(type, fn) { h.set(type, fn); return () => { throw new Error("off-boom"); }; } };',
    '  keep.push(t, h);',
    '  let wr = null;',
    '  const c = mailbox(t, { capacity: 4, words: 1 }, { role: "consumer", mode: "shared", onRecord(x) { wr = new WeakRef(x); } });',
    '  h.get("lw:mb:rec")({ id: 0, v: new Int32Array([1]) });',
    '  if (c.drain() !== 1) throw new Error("drain");',
    '  if (dispose) { try { c.dispose(); } catch (e) {} }',
    '  for (let i = 0; i < 8; i++) { globalThis.gc(); await new Promise((r) => setTimeout(r, 5)); }',
    '  return wr.deref() !== undefined;',
    '}',
    'const disposedAlive = await probe(true), liveAlive = await probe(false);',
    'console.log(JSON.stringify({ disposedAlive, liveAlive, kept: keep.length }));',
  ].join("\n");
  const r = spawnSync(process.execPath, ["--expose-gc", "--input-type=module", "-e", src, pathToFileURL(fileURLToPath(import.meta.url).replace(/test[\\/][^\\/]+$/, "Worker.js")).href],
    { encoding: "utf8", timeout: 60000 });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(JSON.parse(r.stdout.trim()), { disposedAlive: false, liveAlive: true, kept: 4 });
});

// A8: reviewer probes PA..PF (round 2), ported verbatim in substance. Mutants: nakNoOpen, prodDisposeGt0,
// recNoDeadCheck, killNoVd, r2AlwaysFbDrain, fbDrainDeadBreak. Reuses fake6 / heldPair / tap / CFG6 above.
test("B3a A8 PA: consumer-first: a nak opens the producer's window (transfer and auto)", T, () => {
  for (const [pm, cm, ccfg] of [["transfer", "transfer", CFG6], ["auto", "auto", { capacity: 8, words: 1 }]]) {
    const [a, b] = syncPair(true); let n = 0;
    const cons = mailbox(b, ccfg, { role: "consumer", mode: cm, onRecord() { n++; } });
    const prod = mailbox(a, CFG6, { role: "producer", mode: pm });
    assert.equal(prod.mode, "transfer", pm);
    const o = []; for (let i = 0; i < 5; i++) o.push(prod.push([i], 0));
    assert.deepEqual(o, [0, 0, 0, 0, 1], pm);
    assert.equal(cons.drain(), 4);
  }
});
test("B3a A8 PB: a REJECTED producer's dispose() frees the id and its listeners", T, () => {
  const [a, b] = syncPair(true);
  const prod = mailbox(a, CFG6, { role: "producer", mode: "shared" });
  mailbox(b, { capacity: 8, words: 1 }, { role: "consumer", mode: "shared", onRecord() {} });
  assert.equal(prod.push([1], 0), 5);
  prod.dispose();
  for (const k of ["lw:mb:hello", "lw:mb:ack", "lw:mb:nak"]) assert.equal((a._t.get(k) || new Set()).size, 0, k);
  assert.equal(a.__lwMb.producer.size, 0);
  assert.doesNotThrow(() => mailbox(a, CFG6, { role: "producer", mode: "shared" }).dispose());
});
test("B3a A8 PC: a disposed / killed consumer's leftover rec listener never reads the message", T, () => {
  const ft = fake6();
  const c = mailbox(ft, CFG6, { role: "consumer", mode: "transfer", onRecord() {} });
  assert.throws(() => c.dispose(), /off-boom/);
  let reads = 0;
  ft.h.get("lw:mb:rec")({ get id() { reads++; return 0; }, v: new Int32Array([1]) });
  assert.equal(reads, 0, "dispose");
  const [a, b] = syncPair(true);
  const k = mailbox(b, CFG6, { role: "consumer", mode: "transfer", onRecord() {} });
  a.post("lw:mb:rec", { id: 0, v: [1] });   // malformed: kill
  a.post("lw:mb:rec", { get id() { reads++; return 0; }, v: new Int32Array([1]) });
  assert.equal(reads, 0, "kill");
  assert.equal(k.drain(), 0);
});
test("B3a A8 PD: a lane killed before the verdict never attaches to a later valid offer", T, () => {
  const hp = heldPair(["lw:mb:sab"]), blog = tap(hp.b);
  const prod = mailbox(hp.a, CFG6, { role: "producer", mode: "shared" });
  const cons = mailbox(hp.b, CFG6, { role: "consumer", mode: "shared", onRecord() {} });
  hp.a.post("lw:mb:rec", { id: 0, v: [1] });   // malformed: kill
  hp.release("lw:mb:sab");
  assert.equal(cons.attached, false);
  assert.equal(prod.attached, false);
  assert.equal(blog.filter((e) => e.type === "lw:mb:ack" || e.type === "lw:mb:nak").length, 0);
});
test("B3a A8 PE: an attached lane's drain posts no ack when nothing is owed", T, () => {
  const [a, b] = syncPair(true), blog = tap(b);
  const prod = mailbox(a, CFG6, { role: "producer", mode: "shared" });
  const cons = mailbox(b, CFG6, { role: "consumer", mode: "shared", onRecord() {} });
  const acks = () => blog.filter((e) => e.type === "lw:mb:ack").length;
  const a0 = acks();
  assert.equal(cons.drain(), 0);
  assert.equal(prod.push([1], 0), 0);
  assert.equal(cons.drain(), 1);
  assert.equal(acks(), a0, "SAB drains post no credit ack");
});
test("B3a A8 PF: dispose / kill inside onRecord during a fallback drain posts no ack", T, () => {
  for (const how of ["dispose", "kill"]) {
    const [a, b] = syncPair(true), blog = tap(b); let cons, calls = 0;
    const prod = mailbox(a, CFG6, { role: "producer", mode: "transfer" });
    cons = mailbox(b, CFG6, { role: "consumer", mode: "transfer", onRecord() {
      if (++calls > 5) throw new Error("runaway drain");   // a broken engine would loop forever
      if (how === "dispose") cons.dispose(); else a.post("lw:mb:rec", { id: 0, v: [1] });
    } });
    for (let i = 1; i <= 3; i++) prod.push([i], 0);
    assert.equal(cons.drain(), 1);
    assert.equal(blog.filter((e) => e.type === "lw:mb:ack").length, 0, how);
  }
});
