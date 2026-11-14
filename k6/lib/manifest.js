// manifest.js
//
// Loads and validates the fixture manifest (see ../fixtures/manifest.example.json
// for the schema and k6/README.md for the field contract). Deliberately
// strict: a fixture is only usable by a workload pool when it has been
// verified (real bytes/checksum observed against the actual object, not
// assumed), unless the caller explicitly opts into unverified fixtures for
// a smoke/dev run. This prevents silently reporting capacity numbers from
// placeholder data that was never confirmed to exist.
//
// k6 has no filesystem write and no shared mutable state across VUs, so
// `load()` is called once per VU at init time; given the same manifest file
// and env configuration every VU reaches the identical result.

const REQUIRED_FIELDS = ['id', 'format', 'size_class', 'url', 'bytes', 'sha256', 'verified'];
const VALID_SIZE_CLASSES = ['small', 'large'];

// load reads and validates the manifest at `path` (relative to the script
// that calls open(), per k6 semantics). Throws a descriptive Error on any
// structural problem so the run aborts immediately instead of publishing
// bogus dataset metadata.
export function load(openFn, path) {
  let raw;
  try {
    raw = openFn(path);
  } catch (e) {
    throw new Error(`Failed to open fixture manifest "${path}": ${e.message}`);
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    throw new Error(`Fixture manifest "${path}" is not valid JSON: ${e.message}`);
  }

  if (!Array.isArray(parsed.fixtures) || parsed.fixtures.length === 0) {
    throw new Error(`Fixture manifest "${path}" has no non-empty "fixtures" array`);
  }

  const seenIds = new Set();
  parsed.fixtures.forEach((f, idx) => {
    REQUIRED_FIELDS.forEach((key) => {
      if (!(key in f)) {
        throw new Error(`Fixture manifest "${path}": fixtures[${idx}] (id=${f.id || '?'}) is missing required field "${key}"`);
      }
    });
    if (seenIds.has(f.id)) {
      throw new Error(`Fixture manifest "${path}": duplicate fixture id "${f.id}"`);
    }
    seenIds.add(f.id);
    if (VALID_SIZE_CLASSES.indexOf(f.size_class) === -1) {
      throw new Error(`Fixture manifest "${path}": fixture "${f.id}" has invalid size_class "${f.size_class}" (expected "small" or "large")`);
    }
    if (f.verified === true) {
      if (typeof f.bytes !== 'number' || f.bytes <= 0) {
        throw new Error(`Fixture manifest "${path}": fixture "${f.id}" is marked verified but has no positive "bytes"`);
      }
      if (typeof f.sha256 !== 'string' || f.sha256.length < 32) {
        throw new Error(`Fixture manifest "${path}": fixture "${f.id}" is marked verified but has no usable "sha256"`);
      }
    }
  });

  return parsed;
}

// filterByIds restricts fixtures to the given id list (used by FIXTURE_IDS,
// e.g. to pin a boundary-smoke run to specific under/at/over-16MiB
// fixtures). Throws if any requested id is absent, so a typo fails loudly
// instead of silently running fewer fixtures than intended.
export function filterByIds(fixtures, ids) {
  const byId = new Map(fixtures.map((f) => [f.id, f]));
  const missing = ids.filter((id) => !byId.has(id));
  if (missing.length > 0) {
    throw new Error(`FIXTURE_IDS references unknown fixture id(s): ${missing.join(', ')}`);
  }
  return ids.map((id) => byId.get(id));
}

// buildPool selects the fixtures usable for one workload pool (small/large).
// allowUnverified=false (the default posture) means only fixtures with
// verified:true are eligible, so a capacity run can never silently rest on
// a fixture whose real size/checksum was never confirmed.
export function buildPool(fixtures, sizeClass, allowUnverified) {
  const pool = fixtures.filter((f) => f.size_class === sizeClass && (allowUnverified || f.verified === true));
  if (pool.length === 0) {
    throw new Error(
      `No ${allowUnverified ? '' : 'verified '}fixtures with size_class="${sizeClass}" found in the manifest. ` +
        'Add entries (see k6/fixtures/manifest.example.json), run `go run ./loadtest/tools/verifyfixtures` ' +
        'to confirm real bytes/checksum against MinIO, or set ALLOW_UNVERIFIED_FIXTURES=true for a smoke/dev run only.'
    );
  }
  return pool;
}
