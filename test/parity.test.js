// test/parity.test.js -- the MP tier's deterministic part under `npm test`.
//
// Runs mpTier's (a)-(d) (frozen sha pin, golden checks, trace(frozen) and
// trace(live) twice vs the 1.4.0 golden, the dropped++ control) plus one
// spliced control per ported B0x mutant (parity-trace.mjs CONTROLS), each
// asserting its exact first-diff path. Every control splices the FROZEN text,
// so B1+ edits to Worker.js cannot move its anchor. Retention (e) stays in
// test/torture.mjs: it needs --expose-gc and fails closed without it.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import {
  FROZEN_PATH, FROZEN_SHA256, ALLOWED_NEW, CONTROLS,
  trace, firstDiff, allowNew, splice, importText, mutantSource, loadGolden,
} from "./torture/parity-trace.mjs";

// Every test is bounded: a mutant that leaves a call pending forever must fail,
// not hang the runner.
const T = { timeout: 30000 };

const frozenBytes = readFileSync(FROZEN_PATH);
const frozenText = frozenBytes.toString("utf8");
const FROZEN_URL = pathToFileURL(FROZEN_PATH).href;
const sha = createHash("sha256").update(frozenBytes).digest("hex");

async function golden() {
  const frozen = await import(FROZEN_URL);
  return loadGolden(sha, frozen.VERSION);
}

test("MP (a): frozen copy matches the pinned sha256 and the golden", T, async () => {
  assert.equal(sha, FROZEN_SHA256);
  const g = await golden();
  assert.equal(g.frozenSha256, FROZEN_SHA256);
});

test("MP (b)+(c): trace(frozen) and trace(live) x2 equal the 1.4.0 golden", T, async () => {
  const g = (await golden()).trace;
  const Worker0 = globalThis.Worker;
  assert.equal(firstDiff(await trace(await import(FROZEN_URL)), g, "trace"), null);
  const live = await import("../Worker.js");
  for (let run = 1; run <= 2; run++) {
    assert.equal(firstDiff(allowNew(await trace(live), g), g, "trace"), null, "live run " + run);
  }
  assert.equal(globalThis.Worker, Worker0, "the recording Worker was uninstalled");
});

test("MP (d): dropped++ control diverges at a dropped path; anchor guard fails closed", T, async () => {
  const g = (await golden()).trace;
  assert.throws(() => mutantSource(frozenText.split("dropped++;").join("")), /source drifted/);
  assert.throws(() => mutantSource(frozenText + "\n// dropped++;\n"), /source drifted/);
  const d = firstDiff(await trace(await importText(mutantSource(frozenText))), g, "trace");
  assert.ok(d, "control decorative: the mutant traced equal to the golden");
  assert.equal(d.path, "trace.fc.transfer.f32.latest.after.dropped");
});

for (const c of CONTROLS) {
  test("MP control " + c.id + ": the spliced mutant diverges at " + c.path, T, async () => {
    const g = (await golden()).trace;
    const doubled = frozenText + "\n// " + c.anchor + "\n";
    assert.throws(() => splice(doubled, c.anchor, c.to), /source drifted/);
    const mut = await importText(splice(frozenText, c.anchor, c.to));
    const d = firstDiff(allowNew(await trace(mut), g, Object.freeze([])), g, "trace");
    assert.ok(d, "control decorative: the " + c.id + " mutant traced equal to the golden");
    assert.equal(d.path, c.path, "first diff " + JSON.stringify(d));
  });
}

test("MP ALLOWED_NEW: frozen ['mailbox']; allowNew guards, filters, rejects stale entries", T, async () => {
  const g = (await golden()).trace;
  assert.ok(Array.isArray(ALLOWED_NEW) && Object.isFrozen(ALLOWED_NEW));
  assert.deepEqual([...ALLOWED_NEW], ["mailbox"], "B1 allows exactly mailbox");
  const clone = () => JSON.parse(JSON.stringify(g));
  assert.throws(() => allowNew(clone(), g, ["mailbox"]), /not a frozen array/);
  const arrayLike = Object.freeze({ 0: "mailbox", length: 1 });
  assert.throws(() => allowNew(clone(), g, arrayLike), /not a frozen array/);
  assert.throws(() => allowNew(clone(), g, Object.freeze([1])), /not a string/);
  assert.throws(() => allowNew(clone(), g, Object.freeze(["post"])), /already in the golden/);
  assert.throws(() => allowNew(clone(), g), /on no live surface/);
  assert.equal(firstDiff(allowNew(clone(), g, Object.freeze([])), g, "trace"), null);
  const t = clone();
  t.handle.ctxKeys.splice(2, 0, ["mailbox", "function"]);
  const unlisted = firstDiff(allowNew(JSON.parse(JSON.stringify(t)), g, Object.freeze([])), g, "trace");
  assert.equal(unlisted.path, "trace.handle.ctxKeys[2][0]");
  assert.equal(firstDiff(allowNew(JSON.parse(JSON.stringify(t)), g), g, "trace"), null);
  const bad = clone();
  bad.handle.ctxKeys.splice(2, 0, ["mailbox", "object"]);
  assert.throws(() => allowNew(bad, g), /not a function/);
});
