// test/torture/mailbox.mjs -- the mailbox torture tiers (decisions/0003 T-01,
// M3 plan B2). Registered by test/torture.mjs after MP:
//
//   M1  L1 uncontended: main + 1 real worker, 4 SPSC lanes (i32 / f64, both
//       directions), 100,000 records per lane, exact per-record oracle.
//   M3  layout conformance of mb-layout.mjs against live lanes, then L3 wrap:
//       a worker producer started at HEAD = TAIL = 2^31 - 4 runs 4 x CAP
//       records across the Int32 wrap (TAIL ends negative).
//   M4  L4 slow consumer: single thread, CAP accepted then FULL, 100,000 mixed
//       ops, FULL accounting, RETIRED after dispose, the `slots` tripwire, and
//       a retention pass with a pinned-lane control.
//   MC  controls: SPLICE mutants of the live Worker.js text (C3 wrap compare,
//       C4 FULL return removed), each against its unmutated twin. A control
//       that does not fail its predicate is decorative and FAILs the tier.
//
// The predicates below are shared by the tiers AND the controls (the
// torture.mjs shared-predicate convention), so a control exercises the exact
// check the tier relies on. Every tier runs a fixed op count; the 30 s
// no-progress limit is a FAIL timeout, not a measurement.

import { Worker as NodeWorker } from "node:worker_threads";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { mailbox } from "../../Worker.js";
import { syncPair, xorshift32, settle, makeTracker, NOOP_CLEANUP, nodeThreadTransport } from "./harness.mjs";
import { splice, importText } from "./parity-trace.mjs";
import * as L from "./mb-layout.mjs";
import {
  CTL, CTL_WORDS, W_DONE, LIMIT_MS, SALT, L1, L3,
  producerState, consumerState, pushOne, drainOne,
} from "./mb-entry.mjs";

// harness.mjs shadows the global URL, so resolve paths via import.meta.resolve.
const MB_ENTRY = fileURLToPath(import.meta.resolve("./mb-entry.mjs"));
const WORKER_JS = fileURLToPath(import.meta.resolve("../../Worker.js"));

const log = (s) => process.stderr.write(s + "\n");
const SEED = process.env.TORTURE_SEED ? (parseInt(process.env.TORTURE_SEED, 10) | 0) : 0x1a2b3c4d;
const SAB_OK = typeof SharedArrayBuffer !== "undefined" && typeof Atomics !== "undefined";
const immediate = () => new Promise((r) => setImmediate(r));

// SPLICE anchors (decisions/0003 B1 note), each exactly once in Worker.js.
const A_FULL = "if (((t - h) | 0) === cap) return 1;";
const A_WRAP = "((t - h) | 0)";

// ---------------------------------------------------------------------------
// Shared predicates.
// ---------------------------------------------------------------------------
/** Exact SPSC oracle: every accepted record delivered once, in order, intact. */
export const oracleOk = (accepted, delivered, mismatches) => accepted === delivered && mismatches === 0;
/** FULL accounting: every attempt is OK or FULL, and every OK is delivered. */
export const fullAcctOk = (accepted, full, attempted, delivered) => accepted + full === attempted && accepted === delivered;
/** Q7 coverage floor: every op kind >= 1000 per run, every outcome >= 1. */
export const floorOk = (ops, outcomes) => ops.every((n) => n >= 1000) && outcomes.every((n) => n >= 1);

function fail(msg, op) {
  const e = new Error(msg);
  e.seed = SEED;
  if (op !== undefined) e.op = op;
  return e;
}

// ---------------------------------------------------------------------------
// Real-thread plumbing: one mb-entry worker, its transport, a ctl SAB, and a
// FAIL on the worker's 'error' / 'exit' or on 30 s without progress.
// ---------------------------------------------------------------------------
function spawnEntry(lane) {
  const ctlSab = new SharedArrayBuffer(CTL_WORDS * 4);
  const ctl = new Int32Array(ctlSab);
  const worker = new NodeWorker(MB_ENTRY, { workerData: { mode: "run", lane, seed: SEED, ctl: ctlSab } });
  let err = null, exited = false;
  worker.on("error", (e) => { err = e; });
  worker.on("exit", () => { exited = true; });
  const transport = nodeThreadTransport(worker);
  const check = (what) => {
    if (err) throw fail(lane + " " + what + ": worker error: " + (err && err.message));
    if (exited) throw fail(lane + " " + what + ": worker exited early");
  };
  return { worker, transport, ctl, check };
}

async function waitUntil(run, pred, what) {
  const deadline = Date.now() + LIMIT_MS;
  while (!pred()) {
    run.check(what);
    if (Date.now() > deadline) throw fail(what + ": no progress for 30 s (FAIL timeout)");
    await immediate();
  }
  run.check(what);
}

function signal(ctl, idx, v) {
  Atomics.store(ctl, idx, v);
  Atomics.notify(ctl, idx);
}

// ---------------------------------------------------------------------------
// M1 -- L1 uncontended, main + 1 worker, 4 lanes.
// ---------------------------------------------------------------------------
export async function m1Tier() {
  if (!SAB_OK) throw fail("M1: no SharedArrayBuffer/Atomics (fail closed)");
  const { CAP, W, N } = L1;
  const run = spawnEntry("L1");
  const { worker, transport: t, ctl } = run;
  try {
    const prods = L1.M2W.map(({ id, type }) =>
      producerState(mailbox(t, { capacity: CAP, words: W, type }, { role: "producer", mode: "shared", id }),
        SEED ^ SALT.L1_KEY ^ id, W, type));
    const cons = L1.W2M.map(({ id, type }) => {
      const C = consumerState(SEED ^ SALT.L1_KEY ^ id, W, type);
      C.lane = mailbox(t, { capacity: CAP, words: W, type }, { role: "consumer", mode: "shared", id, onRecord: C.onRecord });
      return C;
    });
    await waitUntil(run, () => cons[0].lane.attached && cons[1].lane.attached && Atomics.load(ctl, CTL.W_STAGE) >= 1, "M1 attach");

    // Prologue: main's producers push until FULL; the worker may drain only after.
    const proOk = [0, 0], proFull = [0, 0];
    for (let j = 0; j < 2; j++) {
      const P = prods[j];
      for (let k = 0; k <= CAP && pushOne(P) === mailbox.OK; k++);
      proOk[j] = P.accepted; proFull[j] = P.full;
    }
    signal(ctl, CTL.M_STAGE, 1);
    await waitUntil(run, () => Atomics.load(ctl, CTL.W_STAGE) >= 2, "M1 worker prologue");

    // Mixed phase: the same op stream shape as the worker side.
    const op = xorshift32(SEED ^ SALT.L1_OP);
    let progress = 0, lastProgress = 0, last = Date.now(), iter = 0;
    for (;;) {
      let busy = false;
      for (let j = 0; j < 2; j++) {
        const P = prods[j];
        if (P.accepted >= N) continue;
        busy = true;
        const len = 1 + ((op() * CAP) | 0);
        for (let k = 0; k < len && P.accepted < N; k++) {
          if (pushOne(P) !== mailbox.OK) break;
          progress++;
        }
      }
      for (let j = 0; j < 2; j++) {
        const C = cons[j];
        if (C.delivered >= N) continue;
        busy = true;
        progress += drainOne(C);
      }
      if (!busy) break;
      if ((++iter & 63) === 0) {
        run.check("M1 mixed");
        const now = Date.now();
        if (progress !== lastProgress) { lastProgress = progress; last = now; }
        else if (now - last > LIMIT_MS) throw fail("M1 mixed: no progress for 30 s (FAIL timeout)", iter);
        await immediate();
      }
    }
    await waitUntil(run, () => Atomics.load(ctl, CTL.W_STAGE) === W_DONE, "M1 worker tallies");

    // Per lane: [id, type, accepted, full, pushes, proOk, proFull, delivered, mismatches, drains].
    const rows = [];
    for (let j = 0; j < 2; j++) {
      const P = prods[j], wd = Atomics.load(ctl, CTL.DELIVERED + j);
      rows.push([L1.M2W[j].id, L1.M2W[j].type, P.accepted, P.full, P.pushes, proOk[j], proFull[j],
        wd, Atomics.load(ctl, CTL.MISMATCH + j), Atomics.load(ctl, CTL.DRAINS + j)]);
    }
    for (let j = 0; j < 2; j++) {
      const C = cons[j], acc = Atomics.load(ctl, CTL.ACCEPTED + j), full = Atomics.load(ctl, CTL.FULL + j);
      rows.push([L1.W2M[j].id, L1.W2M[j].type, acc, full, acc + full,
        Atomics.load(ctl, CTL.PRO_OK + j), Atomics.load(ctl, CTL.PRO_FULL + j),
        C.delivered, C.mismatches, C.drains]);
    }
    for (const [id, type, acc, full, pushes, pOk, pFull, del, mis, drains] of rows) {
      log("    M1 lane " + id + " " + type + ": delivered=" + del + " accepted=" + acc + " FULL=" + full +
        " pushes=" + pushes + " prologue=" + pOk + "+" + pFull + " mismatches=" + mis + " drains=" + drains);
      const tag = "M1 lane " + id + " (" + type + ")";
      if (pOk !== CAP || pFull !== 1) throw fail(tag + ": prologue " + pOk + " OK + " + pFull + " FULL != " + CAP + " + 1");
      if (del !== N) throw fail(tag + ": delivered " + del + " != " + N);
      if (!oracleOk(acc, del, mis)) throw fail(tag + ": oracle accepted=" + acc + " delivered=" + del + " mismatches=" + mis);
      if (pushes < N) throw fail(tag + ": pushes " + pushes + " < " + N);
      if (!floorOk([drains], [acc, full])) throw fail(tag + ": coverage floor drains=" + drains + " (>= 1000) OK=" + acc + " FULL=" + full + " (>= 1)");
    }
    for (const C of cons) {
      if (C.mismatches !== 0) log("    M1 first mismatching record on main: " + C.firstBad);
    }
    for (const P of prods) P.lane.dispose();
    for (const C of cons) C.lane.dispose();
  } finally {
    await worker.terminate();
  }
}

// ---------------------------------------------------------------------------
// M3 -- mb-layout conformance against live lanes, then L3 wrap (real thread).
// ---------------------------------------------------------------------------
function conformance(cap, w, type) {
  const tag = "M3 conformance (" + cap + "," + w + "," + type + ")";
  const [pt, ct] = syncPair(true);
  let sab = null;
  ct.on("lw:mb:sab", (m) => { if (m && m.id === 0) sab = m.sab; });
  const layout = { capacity: cap, words: w, type };
  const prod = mailbox(pt, layout, { role: "producer", mode: "shared" });
  const offs = [];
  const cons = mailbox(ct, layout, { role: "consumer", mode: "shared", onRecord: (v, off) => { offs.push(off); } });
  if (!cons.attached || sab === null) throw fail(tag + ": the lane never attached / the SAB was not tapped");
  const want = L.stride(cap, w, type);
  if (prod.byteLength !== want || cons.byteLength !== want || sab.byteLength !== want) {
    throw fail(tag + ": byteLength prod=" + prod.byteLength + " cons=" + cons.byteLength + " sab=" + sab.byteLength + " != stride " + want);
  }
  const h = L.hdr(sab);
  const cold = [[L.GEN, 1, "GEN"], [L.STATE, 0, "STATE"], [L.CAP, cap, "CAP"], [L.W, w, "W"],
    [L.TYPE, L.typeCode(type), "TYPE"], [L.SLOTS, 0, "SLOTS"], [L.SLOT_BYTES, 0, "SLOT_BYTES"],
    [L.FBN, 0, "FBN"], [L.HEAD, 0, "HEAD"], [L.TAIL, 0, "TAIL"], [L.ARMED, 0, "ARMED"]];
  for (const [ix, v, name] of cold) {
    const got = Atomics.load(h, ix);
    if (got !== v) throw fail(tag + ": " + name + " (word " + ix + ") = " + got + " != " + v);
  }
  const pv = type === "f64" ? new Float64Array(sab, L.PAYLOAD, cap * w) : new Int32Array(sab, L.PAYLOAD, cap * w);
  const src = type === "f64" ? new Float64Array(w) : new Int32Array(w);
  const stepAt = (pos, base) => {
    for (let k = 0; k < w; k++) src[k] = base + k;
    const r = prod.push(src, 0);
    if (r !== mailbox.OK) throw fail(tag + ": push at position " + pos + " returned " + r);
    const tail = Atomics.load(h, L.TAIL), head = Atomics.load(h, L.HEAD);
    if (tail !== pos + 1 || head !== pos) throw fail(tag + ": after a push at " + pos + " TAIL=" + tail + " HEAD=" + head + " != " + (pos + 1) + "/" + pos);
    const e = L.elemOf(pos, cap, w);
    for (let k = 0; k < w; k++) {
      if (pv[e + k] !== base + k) throw fail(tag + ": record at position " + pos + " not at element " + e + " (+" + k + ": " + pv[e + k] + " != " + (base + k) + ")");
    }
    const n = cons.drain(), hd = Atomics.load(h, L.HEAD);
    if (n !== 1 || hd !== pos + 1) throw fail(tag + ": drain delivered " + n + " HEAD=" + hd + " != 1/" + (pos + 1));
    if (offs[offs.length - 1] !== e) throw fail(tag + ": onRecord offset " + offs[offs.length - 1] + " != elemOf " + e);
  };
  stepAt(0, 1000);
  L.setPositions(h, 5);
  stepAt(5, 2000);
  prod.dispose();
  if (Atomics.load(h, L.STATE) !== 1) throw fail(tag + ": STATE " + Atomics.load(h, L.STATE) + " != 1 after producer dispose()");
  cons.dispose();
  return want;
}

export async function m3Tier() {
  if (!SAB_OK) throw fail("M3: no SharedArrayBuffer/Atomics (fail closed)");
  const strides = [conformance(1, 1, "i32"), conformance(8, 3, "i32"), conformance(32, 3, "f64"), conformance(64, 5, "f64")];
  log("    M3 conformance strides=" + strides.join("/"));
  if (strides.join("/") !== "640/640/1280/3072") throw fail("M3: strides " + strides.join("/") + " != 640/640/1280/3072");

  // L3: worker producer id 5; main is the consumer and owns the start state.
  const { ID, CAP, W, TYPE, START, ROUNDS } = L3;
  const run = spawnEntry("L3");
  const { worker, transport: t, ctl } = run;
  try {
    let sab = null;
    t.on("lw:mb:sab", (m) => { if (m && m.id === ID) sab = m.sab; });   // the tap
    const C = consumerState(SEED ^ SALT.L3_KEY, W, TYPE);
    C.lane = mailbox(t, { capacity: CAP, words: W, type: TYPE }, { role: "consumer", mode: "shared", id: ID, onRecord: C.onRecord });
    await waitUntil(run, () => C.lane.attached && sab !== null && Atomics.load(ctl, CTL.W_STAGE) >= 1, "M3 L3 attach");
    const h = L.hdr(sab);
    L.setPositions(h, START);              // both sides idle: the worker waits on M_STAGE
    signal(ctl, CTL.M_STAGE, 1);
    for (let r = 1; r <= ROUNDS; r++) {
      await waitUntil(run, () => Atomics.load(ctl, CTL.W_STAGE) >= 1 + r, "M3 L3 round " + r);
      const n = drainOne(C);
      if (n !== CAP) throw fail("M3 L3 round " + r + ": drained " + n + " != " + CAP, r);
      signal(ctl, CTL.M_STAGE, 1 + r);
    }
    await waitUntil(run, () => Atomics.load(ctl, CTL.W_STAGE) === W_DONE, "M3 L3 done");
    const acc = Atomics.load(ctl, CTL.ACCEPTED), full = Atomics.load(ctl, CTL.FULL);
    const tail = Atomics.load(h, L.TAIL), head = Atomics.load(h, L.HEAD), endPos = (START + ROUNDS * CAP) | 0;
    log("    M3 L3 wrap: start=" + START + " accepted=" + acc + " FULL=" + full + " delivered=" + C.delivered +
      " mismatches=" + C.mismatches + " TAIL=" + tail + " HEAD=" + head);
    if (!oracleOk(acc, C.delivered, C.mismatches)) throw fail("M3 L3: oracle accepted=" + acc + " delivered=" + C.delivered + " mismatches=" + C.mismatches);
    if (C.delivered !== ROUNDS * CAP) throw fail("M3 L3: delivered " + C.delivered + " != " + ROUNDS * CAP);
    if (full !== ROUNDS) throw fail("M3 L3: FULL " + full + " != " + ROUNDS);
    if (tail !== endPos || head !== endPos) throw fail("M3 L3: TAIL=" + tail + " HEAD=" + head + " != " + endPos);
    if (!(tail < 0)) throw fail("M3 L3: TAIL " + tail + " never went negative (the wrap was not crossed)");
    C.lane.dispose();
  } finally {
    await worker.terminate();
  }
}

// ---------------------------------------------------------------------------
// L4 / C3 schedules over any mailbox factory (live, spliced, or twin text).
// ---------------------------------------------------------------------------
const L4 = Object.freeze({ CAP: 16, W: 2, TYPE: "i32", OPS: 100000, CYCLES: 512 });

function l4Lane(mb) {
  const [pt, ct] = syncPair(true);
  const layout = { capacity: L4.CAP, words: L4.W, type: L4.TYPE };
  const P = producerState(mb(pt, layout, { role: "producer", mode: "shared" }), SEED ^ SALT.L4_KEY, L4.W, L4.TYPE);
  const C = consumerState(SEED ^ SALT.L4_KEY, L4.W, L4.TYPE);
  C.lane = mb(ct, layout, { role: "consumer", mode: "shared", onRecord: C.onRecord });
  if (!C.lane.attached) throw fail("L4: the lane never attached");
  return { P, C, pt, ct };
}

/** L4 prologue: CAP + 1 pushes into an undrained lane, then one drain. */
function l4Prologue(P, C) {
  for (let k = 0; k <= L4.CAP; k++) pushOne(P);
  drainOne(C);
  return { accepted: P.accepted, full: P.full, attempted: P.pushes, delivered: C.delivered, mismatches: C.mismatches };
}

/** C3 / L3 single-thread: start at 2^31 - 4, `rounds` x (CAP + 1 pushes, drain). */
function wrapRounds(mb, rounds) {
  const { ID, CAP, W, TYPE, START } = L3;
  const [pt, ct] = syncPair(true);
  let sab = null;
  ct.on("lw:mb:sab", (m) => { if (m && m.id === ID) sab = m.sab; });
  const layout = { capacity: CAP, words: W, type: TYPE };
  const P = producerState(mb(pt, layout, { role: "producer", mode: "shared", id: ID }), SEED ^ SALT.L3_KEY, W, TYPE);
  const C = consumerState(SEED ^ SALT.L3_KEY, W, TYPE);
  C.lane = mb(ct, layout, { role: "consumer", mode: "shared", id: ID, onRecord: C.onRecord });
  if (!C.lane.attached || sab === null) throw fail("C3: the lane never attached");
  const h = L.hdr(sab);
  L.setPositions(h, START);
  for (let r = 0; r < rounds; r++) {
    for (let k = 0; k <= CAP; k++) pushOne(P);
    drainOne(C);
  }
  const out = { accepted: P.accepted, full: P.full, attempted: P.pushes, delivered: C.delivered,
    mismatches: C.mismatches, tail: Atomics.load(h, L.TAIL), head: Atomics.load(h, L.HEAD) };
  P.lane.dispose(); C.lane.dispose();
  return out;
}

// ---------------------------------------------------------------------------
// M4 -- L4 slow consumer, single thread.
// ---------------------------------------------------------------------------
// Track at creation, untrack after dispose() (the MP convention): size() 0
// proves every lane was released, the WeakRefs prove every lane was collected.
// With `pin`, the producer is kept alive and never released (the control).
function retentionCycle(tracker, refs, i, pin) {
  const [pt, ct] = syncPair(true);
  const layout = { capacity: L4.CAP, words: L4.W, type: L4.TYPE };
  const prod = mailbox(pt, layout, { role: "producer", mode: "shared", id: 7 });
  let sum = 0;
  const cons = mailbox(ct, layout, { role: "consumer", mode: "shared", id: 7, onRecord: (v, off) => { sum += v[off]; } });
  const recs = [prod, cons, pt, ct].map((o) => { refs.push(new WeakRef(o)); return tracker.track(o, NOOP_CLEANUP, "mb-lane"); });
  const src = new Int32Array(L4.W);
  src[0] = i;
  if (prod.push(src, 0) !== mailbox.OK) throw fail("M4 retention cycle " + i + ": push not OK");
  if (cons.drain() !== 1 || sum !== i) throw fail("M4 retention cycle " + i + ": drain did not deliver the record");
  prod.dispose(); cons.dispose();
  const sizes = pt.__lwMb.producer.size + pt.__lwMb.consumer.size + ct.__lwMb.producer.size + ct.__lwMb.consumer.size;
  if (sizes !== 0) throw fail("M4 retention cycle " + i + ": __lwMb Sets hold " + sizes + " ids after dispose()");
  for (let k = pin ? 1 : 0; k < 4; k++) tracker.untrack(recs[k]);
  if (pin) pin.push(prod);
}

export async function m4Tier() {
  if (!SAB_OK) throw fail("M4: no SharedArrayBuffer/Atomics (fail closed)");
  const { P, C } = l4Lane(mailbox);
  const pro = l4Prologue(P, C);
  log("    M4 prologue: OK=" + pro.accepted + " FULL=" + pro.full + " delivered=" + pro.delivered + " mismatches=" + pro.mismatches);
  if (pro.accepted !== L4.CAP || pro.full !== 1 || pro.delivered !== L4.CAP || pro.mismatches !== 0) {
    throw fail("M4 prologue: " + pro.accepted + " OK + " + pro.full + " FULL, delivered " + pro.delivered + " (want " + L4.CAP + " + 1, " + L4.CAP + ")");
  }

  const op = xorshift32(SEED ^ SALT.L4_OP);
  for (let i = 0; i < L4.OPS; i++) {
    if (op() < 0.9) pushOne(P); else drainOne(C);
  }
  drainOne(C);
  P.lane.dispose();
  const r = P.lane.push(P.stage, 0);
  const retired = r === mailbox.RETIRED ? 1 : 0;
  log("    M4 L4: ops=" + L4.OPS + " pushes=" + P.pushes + " OK=" + P.accepted + " FULL=" + P.full +
    " drains=" + C.drains + " delivered=" + C.delivered + " mismatches=" + C.mismatches + " RETIRED=" + retired);
  if (!fullAcctOk(P.accepted, P.full, P.pushes, C.delivered)) {
    throw fail("M4: FULL accounting OK=" + P.accepted + " + FULL=" + P.full + " vs pushes=" + P.pushes + ", delivered=" + C.delivered);
  }
  if (!oracleOk(P.accepted, C.delivered, C.mismatches)) throw fail("M4: oracle accepted=" + P.accepted + " delivered=" + C.delivered + " mismatches=" + C.mismatches);
  if (!floorOk([P.pushes, C.drains], [P.accepted, P.full, retired])) {
    throw fail("M4: coverage floor pushes=" + P.pushes + " drains=" + C.drains + " OK=" + P.accepted + " FULL=" + P.full + " RETIRED=" + retired);
  }
  if (retired !== 1) throw fail("M4: push after dispose() returned " + r + " != RETIRED (" + mailbox.RETIRED + ")");
  C.lane.dispose();

  // Tripwire (B5 replaces it): a nonzero-SLOTS layout is not built, so `slots`
  // is an unknown layout key today.
  let msg = "";
  try {
    mailbox(syncPair(true)[0], { capacity: L4.CAP, words: L4.W, type: L4.TYPE, slots: 4 }, { role: "producer", mode: "shared" });
  } catch (e) { msg = String(e && e.message); }
  if (!msg.includes("layout has unknown key")) throw fail("M4 slots tripwire: got '" + msg + "' -- slots changed; B5 must replace this tripwire");

  // Retention: 512 create / push / drain / dispose cycles collect to 0.
  const { tracker, leaks, warns } = makeTracker();
  const refs = [];
  for (let i = 0; i < L4.CYCLES; i++) retentionCycle(tracker, refs, i, null);
  await settle();
  await settle();
  const alive = refs.filter((w) => w.deref() !== undefined).length;
  const size = tracker.size();
  const findings = tracker.audit();
  // Pinned-lane control: one producer kept alive MUST stay tracked.
  const ctrl = makeTracker();
  const pin = [], pinRefs = [];
  retentionCycle(ctrl.tracker, pinRefs, -1, pin);
  await settle();
  await settle();
  const pinned = ctrl.tracker.size();
  const pinAlive = pinRefs.filter((w) => w.deref() !== undefined).length;
  pin.length = 0;
  log("    M4 retention: cycles=" + L4.CYCLES + " tracked=" + refs.length + " alive=" + alive + " size=" + size +
    " findings=" + findings.length + " leaks=" + leaks.length + " warnings=" + warns.length + " | pinned control size=" + pinned + " alive=" + pinAlive);
  if (alive !== 0) throw fail("M4 retention: " + alive + " of " + refs.length + " tracked objects survived gc");
  if (size !== 0) throw fail("M4 retention: tracker.size() " + size + " != 0 after " + L4.CYCLES + " cycles");
  if (findings.length !== 0) throw fail("M4 retention: " + findings.length + " leak findings");
  if (leaks.length !== 0) throw fail("M4 retention: leaks: " + leaks.join(", "));
  if (warns.length !== 0) throw fail("M4 retention: tracker warnings: " + warns.join(", "));
  if (pinned < 1 || pinAlive < 1) throw fail("M4 retention control: pinned size=" + pinned + " alive=" + pinAlive + " -- the retention gate cannot fail (decorative)");
}

// ---------------------------------------------------------------------------
// MC -- SPLICE controls on the live Worker.js text, each with its twin.
// ---------------------------------------------------------------------------
function mutate(text, anchor, to, id) {
  const m = splice(text, anchor, to);      // 0 or 2+ occurrences: "source drifted"
  if (m === text) throw fail("MC " + id + ": the mutant equals the live text (decorative)");
  return m;
}

export async function mcTier() {
  if (!SAB_OK) throw fail("MC: no SharedArrayBuffer/Atomics (fail closed)");
  const text = readFileSync(WORKER_JS, "utf8");
  const c3Text = mutate(text, A_WRAP, "(t < h + cap ? 0 : cap)", "C3");
  if (!c3Text.includes("if ((t < h + cap ? 0 : cap) === cap) return 1;")) throw fail("MC C3: the splice did not land in the FULL check");
  const c4Text = mutate(text, A_FULL, ";", "C4");
  const twin = (await importText(text)).mailbox;
  const c3 = (await importText(c3Text)).mailbox;
  const c4 = (await importText(c4Text)).mailbox;

  // C3: the wrap compare made a plain `<`. From 2^31 - 4, push 4 wraps TAIL to
  // -2^31 and push 9 is the first FULL check across the wrap.
  const m3 = wrapRounds(c3, 1), t3 = wrapRounds(twin, L3.ROUNDS);
  log("    MC C3 mutant: accepted=" + m3.accepted + " FULL=" + m3.full + " delivered=" + m3.delivered + " mismatches=" + m3.mismatches +
    " TAIL=" + m3.tail + " HEAD=" + m3.head + " | twin: accepted=" + t3.accepted + " FULL=" + t3.full +
    " delivered=" + t3.delivered + " mismatches=" + t3.mismatches + " TAIL=" + t3.tail + " HEAD=" + t3.head);
  if (oracleOk(m3.accepted, m3.delivered, m3.mismatches)) throw fail("MC C3: the wrap mutant passed oracleOk (decorative)");
  const end3 = (L3.START + L3.ROUNDS * L3.CAP) | 0;
  if (!oracleOk(t3.accepted, t3.delivered, t3.mismatches) || t3.delivered !== L3.ROUNDS * L3.CAP || t3.full !== L3.ROUNDS ||
      t3.tail !== end3 || t3.head !== end3) {
    throw fail("MC C3 twin: accepted=" + t3.accepted + " delivered=" + t3.delivered + " FULL=" + t3.full + " TAIL=" + t3.tail + " (want " + (L3.ROUNDS * L3.CAP) + "/" + L3.ROUNDS + "/" + end3 + ")");
  }

  // C4: the FULL return removed, then the L4 prologue.
  const mk = l4Lane(c4), tk = l4Lane(twin);
  const m4 = l4Prologue(mk.P, mk.C), t4 = l4Prologue(tk.P, tk.C);
  mk.P.lane.dispose(); mk.C.lane.dispose(); tk.P.lane.dispose(); tk.C.lane.dispose();
  log("    MC C4 mutant: (accepted, FULL, attempted, delivered)=(" + m4.accepted + "," + m4.full + "," + m4.attempted + "," + m4.delivered +
    ") | twin: (" + t4.accepted + "," + t4.full + "," + t4.attempted + "," + t4.delivered + ")");
  if (fullAcctOk(m4.accepted, m4.full, m4.attempted, m4.delivered)) throw fail("MC C4: the FULL-removed mutant passed fullAcctOk (decorative)");
  if (floorOk([], [m4.full])) throw fail("MC C4: the FULL-removed mutant passed the FULL floor (decorative)");
  if (!fullAcctOk(t4.accepted, t4.full, t4.attempted, t4.delivered) || !floorOk([], [t4.full]) ||
      t4.accepted !== L4.CAP || t4.full !== 1 || t4.delivered !== L4.CAP || t4.mismatches !== 0) {
    throw fail("MC C4 twin: (" + t4.accepted + "," + t4.full + "," + t4.attempted + "," + t4.delivered + ") != (16,1,17,16)");
  }
}
