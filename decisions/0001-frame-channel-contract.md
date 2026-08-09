# 0001 -- frameChannel contract: live-view lifetime and handshake trust

- Status: accepted
- Date: 2026-08-09
- Session: W2 (@zakkster/lite-worker v1.3.3)
- Findings: W-06, W-10

## Context

W1 exercised two frameChannel paths whose contracts were never written down:

1. How long the view returned by a consumer's `read()` stays coherent -- the
   lifetime a renderer implicitly relies on when it uploads straight from the view.
2. How much trust the shared-mode handshake extends to an incoming typed message.
   The `lw:fc:sab` handler ran on the shared typed plane and accepted any object
   with a truthy `sab` field whose byteLength/count matched the layout.

Both were correct-by-accident, not correct-by-contract. This record pins them.

## Decision W-06 -- document the live-view boundary; do NOT add a helper

The view `read()` returns aliases a fixed pool slot. In both transfer and shared
mode it stays valid for exactly `count - 1` further `produce()` calls and is
overwritten on the `count`-th, when the pool wraps back to that slot. A renderer
that consumes the view within the same frame is safe; a consumer that holds it
across a yield is not.

We document this boundary in the `read()` d.ts comment, `README.md`, and
`llms.txt`, and direct holders to `readInto(dst)` (which copies under the seqlock)
for any data that must outlive the read. We pin it by name in the torture suite
(T3 W-06 spot-check; T8 asserts the round-trip in both modes).

We deliberately do NOT add a `count`-aware "is this view still valid" helper.
Rationale:

- The boundary is structural to a fixed, allocation-free pool. A helper would
  imply a safety the pool cannot give -- it can report that a slot has been
  reused, but it cannot stop the reuse or preserve the old contents.
- A validity query invites callers to hold the view and poll, which is exactly
  the retention pattern the pool exists to prevent. `readInto` already gives a
  snapshot that cannot change underneath the caller; that is the supported way to
  retain a frame.
- Zero new API surface keeps the hot path and the type surface unchanged.

## Decision W-10 -- the handshake fails closed; `lw:fc:*` is reserved

The `lw:fc:sab` consumer handler now rejects any message whose `sab` is not a real
`SharedArrayBuffer`. A spoofed handshake carrying a plain `ArrayBuffer` with a
matching byteLength/count is ignored; the consumer stays on the transferable ring
(`mode === "transfer"`, `shared === false`). The distinct layout-mismatch return
stays a separate clause so the two failure modes remain independently observable.

`lw:fc:sab` and `lw:fc:hello` are a reserved namespace, documented exactly as
`lw:canvas*` is: callers must not post those types themselves. This is the only
logic change to `Worker.js` in this release; the `produce`/`read`/`readInto` hot
bodies are byte-for-byte unchanged and the retention gate is unmoved.

## Consequences

- No new transport, no format change, no runtime dependency, no API surface
  change beyond the guard and the docs.
- Fail-closed handshake: an untrusted typed message cannot force a consumer onto
  shared memory it did not allocate.
- Torture T4 (backpressure) and T8 (LAYOUT conformance) fill the two W2 tiers,
  each with a T9 control that must fail if the real code regresses.
