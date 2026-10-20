// test/torture/mb-layout.mjs -- the ONLY test-side copy of the mailbox layout.
//
// Mirrors the LAYOUT comment above makeMailbox in Worker.js (decisions/0002
// M-03, decisions/0003 T-01). The torture drives a live lane's SAB through
// these words directly (L3 wrap, the C3 control), and M3's conformance step
// checks every constant here against a live lane (the T8 precedent), so a
// layout change fails in one place. No slab formula yet: SLOTS > 0 is not
// built (B5 adds it here).
//
// Int32 word indices. Hot words sit 128 B (32 words) apart.

export const HEAD = 0;        // consumer line: next position to read
export const ARMED = 1;       // consumer waits on TAIL when nonzero
export const TAIL = 32;       // producer line: next position to write (the publish)
export const FBN = 33;        // fallback record count (0 on the shared path)
export const GEN = 64;        // cold line: generation, 1 at construction
export const STATE = 65;      // 0 live, 1 retired
export const CAP = 66;        // capacity in records (a power of two)
export const W = 67;          // words per record
export const TYPE = 68;       // 0 i32, 1 f64
export const SLOTS = 69;      // slab slots (0: no slab)
export const SLOT_BYTES = 70; // bytes per slab slot (0: no slab)

export const PAYLOAD = 384;   // payload byte offset
export const HDR_WORDS = 96;  // the three 128 B header lines as Int32 words

export const roundUp128 = (x) => (x + 127) & ~127;
export const eltOf = (type) => (type === "f64" ? 8 : 4);
export const typeCode = (type) => (type === "f64" ? 1 : 0);

/** Lane stride in bytes with SLOTS = 0: header, payload, 128 B trailing gap. */
export function stride(cap, w, type) {
  return PAYLOAD + roundUp128(cap * w * eltOf(type)) + 128;
}

/** The header words of a lane SAB. */
export function hdr(sab) {
  return new Int32Array(sab, 0, HDR_WORDS);
}

/** Payload element index of the record at position `pos`. */
export function elemOf(pos, cap, w) {
  return (pos & (cap - 1)) * w;
}

/** Write HEAD then TAIL = pos. Only while both sides are idle (L3, C3). */
export function setPositions(i32, pos) {
  Atomics.store(i32, HEAD, pos | 0);
  Atomics.store(i32, TAIL, pos | 0);
}
