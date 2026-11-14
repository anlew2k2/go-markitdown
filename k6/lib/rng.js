// rng.js
//
// Deterministic, seeded helpers for reproducible fixture scheduling. Every
// function here is a pure function of its inputs so that every VU (each a
// separate k6 JS runtime) computes the exact same sequence given the same
// manifest + env configuration, without any shared mutable state.

// mulberry32: small, fast, deterministic PRNG. Not cryptographic; good
// enough (and auditable) for test-schedule shuffling.
export function mulberry32(seed) {
  let t = seed >>> 0;
  return function () {
    t += 0x6d2b79f5;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

// hashString: small string -> uint32 hash (FNV-1a), used to derive distinct
// but deterministic per-pool seeds from one top-level seed.
export function hashString(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

// seededShuffle: deterministic Fisher-Yates shuffle of `items` (not
// mutated; returns a new array) using the given integer seed.
export function seededShuffle(items, seed) {
  const arr = items.slice();
  const rand = mulberry32(seed);
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    const tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
  return arr;
}

// isSmallSlot implements a digital-differential-analyzer (Bresenham-style)
// interleave: over any prefix of length N, exactly floor(N * ratioSmall)
// slots are "small". It is a pure function of the slot index, so every VU
// agrees on the class for iteration i without coordination, and the
// configured ratio is honored exactly (not just on average).
export function isSmallSlot(i, ratioSmall) {
  return Math.floor((i + 1) * ratioSmall) - Math.floor(i * ratioSmall) === 1;
}

// isSmallSlotBlock groups iterations into fixed-size blocks and places all
// "large" slots at the front of each block, so large documents arrive in a
// burst ahead of a run of small ones. Useful for deliberately probing
// head-of-line blocking (a large document occupying the single Splitter /
// Extractor process ahead of smalls) instead of the smoothed interleave
// above. Still a pure, seed-free function of (i, ratioSmall, blockSize).
export function isSmallSlotBlock(i, ratioSmall, blockSize) {
  const posInBlock = i % blockSize;
  const largeCountPerBlock = Math.round(blockSize * (1 - ratioSmall));
  return posInBlock >= largeCountPerBlock;
}
