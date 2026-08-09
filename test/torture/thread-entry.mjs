// test/torture/thread-entry.mjs -- real node:worker_threads entry for T3/T5/T6.
//
// Receives the FULL serialized lite-worker source (WORKER_RUNTIME +
// makeFrameChannel + the user producer body, exactly as defineWorker() builds
// it) via workerData and evaluates it UNCHANGED with a `self` shim wired over
// parentPort -- so the ACTUAL off-thread channel code runs on a second OS
// thread, with real SharedArrayBuffer + Atomics. This is what W-04/W-05 need:
// the seqlock exercised against a real writer, not an in-process mock.

import { parentPort, workerData } from "node:worker_threads";
import { installSelfShim } from "./harness.mjs";

// The serialized source is shaped for `new Function("self", source)`: it reads
// the worker globals through `self`, exactly as a browser Worker would. Bind our
// parentPort-backed shim as `self` and run it verbatim.
const selfObj = installSelfShim(parentPort, {});
// eslint-disable-next-line no-new-func
new Function("self", workerData.source)(selfObj);
