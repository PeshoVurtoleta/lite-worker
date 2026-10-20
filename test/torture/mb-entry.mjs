// test/torture/mb-entry.mjs -- the mailbox torture's worker entry AND the
// record generator both sides share (decisions/0003 T-01, plan B2 D3/D4).
//
// Imported by mailbox.mjs on the main thread for its exports (the ctl word map,
// the generator, the compare, the lane constants); run as a node:worker_threads
// entry with workerData { mode: "run", lane: "L1" | "L3", seed, ctl } it drives
// the worker side of L1 / L3 through the REAL `mailbox` export of Worker.js over
// nodeThreadTransport(parentPort). Modes `inject` / `probe` arrive in B4.
//
// Lockstep: `ctl` is a test-side Int32Array(CTL_WORDS) on its own SAB. The
// worker blocks in Atomics.wait on M_STAGE; the main thread never blocks (it
// polls W_STAGE with setImmediate). Both sides fail after 30 s with no progress:
// that is a FAIL timeout, never a measurement.

import { parentPort, workerData } from "node:worker_threads";
import { mailbox } from "../../Worker.js";
import { xorshift32, nodeThreadTransport } from "./harness.mjs";

export const LIMIT_MS = 30000;

// ctl word map. W_* are written by the worker only, M_* by the main thread only.
// The tallies are the worker's per-lane counts, slot +0 / +1 for its two lanes
// (L1: producers ids 3 / 4 and consumers ids 1 / 2; L3: slot +0 only).
export const CTL_WORDS = 16;
export const CTL = Object.freeze({
  W_STAGE: 0,     // worker progress: 1 ready, 2 prologue done (L1) / 1 + round (L3), DONE at the end
  M_STAGE: 1,     // main progress: 1 prologue done (L1) / positions set (L3), then 1 + round (L3)
  PRO_OK: 2,      // producer prologue OK count
  PRO_FULL: 4,    // producer prologue FULL count
  FULL: 6,        // producer FULL total (prologue included)
  DELIVERED: 8,   // consumer delivered total
  MISMATCH: 10,   // consumer mismatches
  DRAINS: 12,     // consumer drain() calls
  ACCEPTED: 14,   // producer OK total (prologue included)
});
export const W_DONE = 0x7fff;

// Salts (decisions/0003 Q7: the lane number in hex). L3 has no op stream.
export const SALT = Object.freeze({
  L1_OP: 0x6d620101, L1_KEY: 0x6d620102,
  L3_KEY: 0x6d620302,
  L4_OP: 0x6d620401, L4_KEY: 0x6d620402,
});

// L1: 4 lanes, ids 1 / 2 main -> worker, 3 / 4 worker -> main.
export const L1 = Object.freeze({
  CAP: 32, W: 3, N: 100000,
  M2W: Object.freeze([Object.freeze({ id: 1, type: "i32" }), Object.freeze({ id: 2, type: "f64" })]),
  W2M: Object.freeze([Object.freeze({ id: 3, type: "i32" }), Object.freeze({ id: 4, type: "f64" })]),
});
// L3: worker producer id 5 started at HEAD = TAIL = 2^31 - 4, ROUNDS x (CAP + 1) pushes.
export const L3 = Object.freeze({ ID: 5, CAP: 8, W: 2, TYPE: "i32", START: 0x7ffffffc, ROUNDS: 4 });

// Word 0 of every 97th record cycles a special. f64 compares with Object.is
// (NaN by Number.isNaN only: payload bits may canonicalise). i32 lanes push
// raw doubles and compare against v | 0 (the lane stores ToInt32).
export const SPECIAL_EVERY = 97;
export const F64_SPECIALS = Object.freeze([-0, Infinity, -Infinity, 2 ** 53, 5e-324, NaN]);
export const I32_SPECIALS = Object.freeze([2 ** 31, -(2 ** 31) - 1, 2 ** 32 - 1, 1.5, -1.5, NaN, Infinity]);

/**
 * The record stream of one lane: xorshift32(s), two draws per word -> a double
 * in (-2^32, 2^32) with a fraction. fill(dst) writes the next record into a
 * Float64Array and advances; the producer refills only after an OK, so a FULL
 * push never advances the stream.
 */
export function makeGen(s, w, type) {
  const next = xorshift32(s);
  const spec = type === "f64" ? F64_SPECIALS : I32_SPECIALS;
  let r = 0;
  return {
    fill(dst) {
      for (let k = 0; k < w; k++) dst[k] = (next() - 0.5) * 8589934592 + next();
      if (r % SPECIAL_EVERY === 0) dst[0] = spec[((r / SPECIAL_EVERY) | 0) % spec.length];
      r++;
    },
    get count() { return r; },
  };
}

/** Does view[off .. off + w) carry the expected record `exp`? */
export function sameRecord(view, off, exp, w, type) {
  for (let k = 0; k < w; k++) {
    const got = view[off + k], want = exp[k];
    if (type === "f64") {
      if (Number.isNaN(want) ? !Number.isNaN(got) : !Object.is(got, want)) return false;
    } else if (got !== (want | 0)) {
      return false;
    }
  }
  return true;
}

/** Producer-side state over a lane built by any mailbox factory (live or spliced). */
export function producerState(lane, s, w, type) {
  return { lane, gen: makeGen(s, w, type), stage: new Float64Array(w), staged: false, pushes: 0, accepted: 0, full: 0 };
}

/** One push of the staged record; returns the outcome. Only OK / FULL are expected here. */
export function pushOne(P) {
  if (!P.staged) { P.gen.fill(P.stage); P.staged = true; }
  const r = P.lane.push(P.stage, 0);
  P.pushes++;
  if (r === mailbox.OK) { P.accepted++; P.staged = false; }
  else if (r === mailbox.FULL) P.full++;
  else throw new Error("mailbox torture: push returned " + r + " (expected OK or FULL) at push " + P.pushes);
  return r;
}

/** Consumer-side state; build the lane with { onRecord: C.onRecord }, then set C.lane. */
export function consumerState(s, w, type) {
  const gen = makeGen(s, w, type), exp = new Float64Array(w);
  const C = { lane: null, seen: 0, delivered: 0, mismatches: 0, firstBad: -1, drains: 0, onRecord: null };
  C.onRecord = (view, off) => {
    gen.fill(exp);
    if (!sameRecord(view, off, exp, w, type)) {
      if (C.mismatches === 0) C.firstBad = C.seen;
      C.mismatches++;
    }
    C.seen++;
  };
  return C;
}

export function drainOne(C) {
  const seen0 = C.seen;
  const n = C.lane.drain();
  C.drains++;
  C.delivered += n;
  if (C.seen - seen0 !== n) C.mismatches++;   // drain() count != onRecord calls
  return n;
}

// ---------------------------------------------------------------------------
// Worker side. Everything below runs only under workerData.mode === "run".
// ---------------------------------------------------------------------------
const immediate = () => new Promise((r) => setImmediate(r));

function waitWord(ctl, idx, atLeast, what) {
  const deadline = Date.now() + LIMIT_MS;
  for (;;) {
    const v = Atomics.load(ctl, idx);
    if (v >= atLeast) return;
    if (Date.now() > deadline) throw new Error("mb-entry " + what + ": no progress for 30 s (FAIL timeout)");
    Atomics.wait(ctl, idx, v, 250);
  }
}

function signal(ctl, idx, v) {
  Atomics.store(ctl, idx, v);
  Atomics.notify(ctl, idx);
}

async function runL1(ctl, seed) {
  const { CAP, W, N } = L1;
  const t = nodeThreadTransport(parentPort);
  const cons = L1.M2W.map(({ id, type }) => {
    const C = consumerState(seed ^ SALT.L1_KEY ^ id, W, type);
    C.lane = mailbox(t, { capacity: CAP, words: W, type }, { role: "consumer", mode: "shared", id, onRecord: C.onRecord });
    return C;
  });
  const prods = L1.W2M.map(({ id, type }) =>
    producerState(mailbox(t, { capacity: CAP, words: W, type }, { role: "producer", mode: "shared", id }),
      seed ^ SALT.L1_KEY ^ id, W, type));
  // The consumers attach when main's lw:mb:sab arrives, the producers when
  // main's attached lw:mb:ack arrives (until then they ride the fallback):
  // yield the event loop.
  const deadline = Date.now() + LIMIT_MS;
  while (!cons[0].lane.attached || !cons[1].lane.attached || !prods[0].lane.attached || !prods[1].lane.attached) {
    if (Date.now() > deadline) throw new Error("mb-entry L1: lanes never attached in 30 s (FAIL timeout)");
    await immediate();
  }
  signal(ctl, CTL.W_STAGE, 1);

  // Prologue: each worker producer pushes until FULL before main may drain it.
  for (let j = 0; j < 2; j++) {
    const P = prods[j];
    for (let k = 0; k <= CAP && pushOne(P) === mailbox.OK; k++);
    Atomics.store(ctl, CTL.PRO_OK + j, P.accepted);
    Atomics.store(ctl, CTL.PRO_FULL + j, P.full);
  }
  signal(ctl, CTL.W_STAGE, 2);
  waitWord(ctl, CTL.M_STAGE, 1, "L1 prologue");

  // Mixed phase: bursts of 1..CAP per producer lane, one drain per consumer lane.
  const op = xorshift32(seed ^ SALT.L1_OP);
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
    if ((++iter & 4095) === 0) {
      const now = Date.now();
      if (progress !== lastProgress) { lastProgress = progress; last = now; }
      else if (now - last > LIMIT_MS) throw new Error("mb-entry L1: no progress for 30 s (FAIL timeout)");
    }
  }
  for (let j = 0; j < 2; j++) {
    Atomics.store(ctl, CTL.FULL + j, prods[j].full);
    Atomics.store(ctl, CTL.ACCEPTED + j, prods[j].accepted);
    Atomics.store(ctl, CTL.DELIVERED + j, cons[j].delivered);
    Atomics.store(ctl, CTL.MISMATCH + j, cons[j].mismatches);
    Atomics.store(ctl, CTL.DRAINS + j, Math.min(cons[j].drains, 0x7fffffff));
  }
  signal(ctl, CTL.W_STAGE, W_DONE);
}

async function runL3(ctl, seed) {
  const { ID, CAP, W, TYPE, ROUNDS } = L3;
  const t = nodeThreadTransport(parentPort);
  const P = producerState(mailbox(t, { capacity: CAP, words: W, type: TYPE }, { role: "producer", mode: "shared", id: ID }),
    seed ^ SALT.L3_KEY, W, TYPE);
  // The producer pushes on the SAB only after main's attached lw:mb:ack.
  const deadline = Date.now() + LIMIT_MS;
  while (!P.lane.attached) {
    if (Date.now() > deadline) throw new Error("mb-entry L3: producer never attached in 30 s (FAIL timeout)");
    await immediate();
  }
  signal(ctl, CTL.W_STAGE, 1);
  // Main attaches, taps the SAB and writes HEAD = TAIL = START while we wait.
  waitWord(ctl, CTL.M_STAGE, 1, "L3 positions");
  for (let r = 1; r <= ROUNDS; r++) {
    // Push until FULL, bounded at CAP + 1 attempts (a broken FULL check cannot spin).
    for (let k = 0; k <= CAP && pushOne(P) === mailbox.OK; k++);
    Atomics.store(ctl, CTL.ACCEPTED, P.accepted);
    Atomics.store(ctl, CTL.FULL, P.full);
    signal(ctl, CTL.W_STAGE, 1 + r);
    waitWord(ctl, CTL.M_STAGE, 1 + r, "L3 round " + r);
  }
  signal(ctl, CTL.W_STAGE, W_DONE);
}

if (workerData?.mode === "run") {
  const { lane, seed, ctl } = workerData;
  const i32 = new Int32Array(ctl);
  try {
    if (lane === "L1") await runL1(i32, seed | 0);
    else if (lane === "L3") await runL3(i32, seed | 0);
    else throw new Error("mb-entry: unknown lane '" + lane + "' (known: L1, L3)");
  } catch (e) {
    // Surface as the worker's 'error' event on the main thread (a FAIL there).
    process.nextTick(() => { throw e; });
  }
}
