// load_test.js
//
// Event-Driven Document Pipeline load test. See k6/README.md for the full
// configuration reference, example commands and known limitations.
//
// This script ONLY produces arrival traffic and client-side observation. It
// deliberately does NOT claim to measure authoritative throughput or to
// prove the backend has drained: that requires the post-run reconciliation
// tool (`loadtest/tools/drain`), which reads stream:doc-extractor:output
// directly and joins it against this script's structured stdout log and the
// fixture manifest. Run `loadtest/tools/drain record` before this script and
// `loadtest/tools/drain run` after gracefulStop elapses.
import { sleep } from 'k6';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';
import redis from 'k6/x/redis';

import * as manifestLib from './lib/manifest.js';
import { seededShuffle, hashString, isSmallSlot, isSmallSlotBlock } from './lib/rng.js';
import { parseRedisConfig, redactUrl } from './lib/redisClient.js';
import { buildStages, startRateOf, validateVuAllocation } from './lib/stages.js';

// ---------------------------------------------------------------------------
// Custom Metrics
//
// Tags are limited to {workload, size_class, format} by design (see
// docs/load-test-plan-event-sourcing.md section 4): request_id must never
// become a metric tag, since k6 would create one time series per request.
// ---------------------------------------------------------------------------
const acceptedPublishes = new Counter('accepted_publishes');
const publishErrors = new Counter('publish_errors'); // PUBLISH_UNKNOWN
const observedSuccesses = new Counter('observed_successes');
const terminalFailures = new Counter('terminal_failures');
const observationTimeouts = new Counter('observation_timeouts');
const redisReadErrors = new Counter('redis_read_errors');
const admissionStopped = new Counter('admission_stopped');
// e2e_duration / failed_duration are recorded ONLY for their own outcome
// (COMPLETED / FAILED respectively). Always read them next to the
// terminal_failures / observation_timeouts / admission_stopped counts for
// the same {workload, size_class, format}: a p95 computed only from
// COMPLETED samples is survivorship-biased if a meaningful fraction of the
// same cohort failed, timed out, or was never admitted.
const e2eDuration = new Trend('e2e_duration', true);
const failedDuration = new Trend('failed_duration', true);

// ---------------------------------------------------------------------------
// Configuration & validation (runs once per VU runtime at init time, so a
// bad config aborts every VU immediately with a clear message instead of
// failing unpredictably mid-run).
// ---------------------------------------------------------------------------
const WORKLOADS = ['small', 'large', 'mixed'];

function readConfig(env) {
  const workload = env.WORKLOAD;
  if (WORKLOADS.indexOf(workload) === -1) {
    throw new Error(`WORKLOAD must be one of ${WORKLOADS.join(', ')} (got "${workload}"). Example: k6 run -e WORKLOAD=small-only ... is wrong; use -e WORKLOAD=small.`);
  }

  const mixSmallRatio = env.MIX_SMALL_RATIO !== undefined ? parseFloat(env.MIX_SMALL_RATIO) : 0.8;
  if (workload === 'mixed' && (!(mixSmallRatio > 0) || !(mixSmallRatio < 1))) {
    throw new Error(`MIX_SMALL_RATIO must be strictly between 0 and 1 (got "${env.MIX_SMALL_RATIO}"). Default assumption, undocumented by product data, is 0.8 (80% small / 20% large by document count).`);
  }

  const mixedOrder = env.MIXED_ORDER || 'interleaved';
  if (workload === 'mixed' && ['interleaved', 'block'].indexOf(mixedOrder) === -1) {
    throw new Error(`MIXED_ORDER must be "interleaved" or "block" (got "${mixedOrder}")`);
  }
  const mixedBlockSize = env.MIXED_BLOCK_SIZE ? parseInt(env.MIXED_BLOCK_SIZE, 10) : 10;
  if (mixedOrder === 'block' && (!Number.isFinite(mixedBlockSize) || mixedBlockSize < 2)) {
    throw new Error(`MIXED_BLOCK_SIZE must be an integer >= 2 (got "${env.MIXED_BLOCK_SIZE}")`);
  }

  const seedRaw = env.WORKLOAD_SEED !== undefined ? env.WORKLOAD_SEED : '20261114';
  const seed = parseInt(seedRaw, 10);
  if (!Number.isFinite(seed)) {
    throw new Error(`WORKLOAD_SEED must be an integer (got "${seedRaw}")`);
  }

  const outstandingLimit = env.OUTSTANDING_LIMIT ? parseInt(env.OUTSTANDING_LIMIT, 10) : 20;
  if (!Number.isFinite(outstandingLimit) || outstandingLimit < 1) {
    throw new Error(`OUTSTANDING_LIMIT must be a positive integer (got "${env.OUTSTANDING_LIMIT}")`);
  }

  const pollIntervalMs = env.POLL_INTERVAL_MS ? parseInt(env.POLL_INTERVAL_MS, 10) : 1000;
  const maxObservationSeconds = env.MAX_OBSERVATION_SECONDS ? parseInt(env.MAX_OBSERVATION_SECONDS, 10) : 600;

  return {
    workload,
    envPrefix: workload.toUpperCase(),
    manifestPath: env.MANIFEST_PATH || './fixtures/manifest.example.json',
    allowUnverifiedFixtures: env.ALLOW_UNVERIFIED_FIXTURES === 'true',
    fixtureIds: env.FIXTURE_IDS ? env.FIXTURE_IDS.split(',').map((s) => s.trim()).filter(Boolean) : null,
    mixSmallRatio,
    mixedOrder,
    mixedBlockSize,
    seed,
    outstandingLimit,
    redisUrl: env.REDIS_URL || 'redis://127.0.0.1:6379',
    redisDialTimeout: env.REDIS_DIAL_TIMEOUT ? parseInt(env.REDIS_DIAL_TIMEOUT, 10) : 5,
    redisReadTimeout: env.REDIS_READ_TIMEOUT ? parseInt(env.REDIS_READ_TIMEOUT, 10) : 5,
    redisWriteTimeout: env.REDIS_WRITE_TIMEOUT ? parseInt(env.REDIS_WRITE_TIMEOUT, 10) : 5,
    inputStream: env.INPUT_STREAM || 'stream:doc-extractor:input',
    stateKeyPrefix: env.STATE_KEY_PREFIX || 'doc-extractor:state:',
    pollIntervalMs,
    maxObservationSeconds,
    guardrailPrefix: env.GUARDRAIL_KEY_PREFIX || 'loadtest:',
  };
}

const config = readConfig(__ENV);

// ---------------------------------------------------------------------------
// Fixture manifest & deterministic workload pools
// ---------------------------------------------------------------------------
const manifestDoc = manifestLib.load(open, config.manifestPath);
let fixtures = manifestDoc.fixtures;
if (config.fixtureIds) {
  fixtures = manifestLib.filterByIds(fixtures, config.fixtureIds);
}

function buildOrder(sizeClass) {
  const pool = manifestLib.buildPool(fixtures, sizeClass, config.allowUnverifiedFixtures);
  const poolSeed = (config.seed ^ hashString(sizeClass)) >>> 0;
  return seededShuffle(pool, poolSeed);
}

const smallOrder = config.workload !== 'large' ? buildOrder('small') : null;
const largeOrder = config.workload !== 'small' ? buildOrder('large') : null;

function rotate(order, i) {
  return order[i % order.length];
}

// datasetForIteration is a pure function of the global iteration index, so
// every VU picks the identical fixture for a given slot without
// coordination. See docs/load-test-plan-event-sourcing.md section 3 and
// k6/README.md for why this (not __ITER, which is local per VU) is required
// for a reproducible, auditable mix.
function datasetForIteration(i) {
  if (config.workload === 'small') return rotate(smallOrder, i);
  if (config.workload === 'large') return rotate(largeOrder, i);
  const wantSmall =
    config.mixedOrder === 'block'
      ? isSmallSlotBlock(i, config.mixSmallRatio, config.mixedBlockSize)
      : isSmallSlot(i, config.mixSmallRatio);
  return wantSmall ? rotate(smallOrder, i) : rotate(largeOrder, i);
}

// ---------------------------------------------------------------------------
// Stage schedule & VU allocation check
// ---------------------------------------------------------------------------
const stages = buildStages(__ENV, config.envPrefix);
const startRate = __ENV[`${config.envPrefix}_START_RATE`] ? parseInt(__ENV[`${config.envPrefix}_START_RATE`], 10) : startRateOf(stages);
const preAllocatedVUs = __ENV[`${config.envPrefix}_PRE_ALLOCATED_VUS`]
  ? parseInt(__ENV[`${config.envPrefix}_PRE_ALLOCATED_VUS`], 10)
  : __ENV.PRE_ALLOCATED_VUS
    ? parseInt(__ENV.PRE_ALLOCATED_VUS, 10)
    : 30;
const maxVUs = __ENV[`${config.envPrefix}_MAX_VUS`]
  ? parseInt(__ENV[`${config.envPrefix}_MAX_VUS`], 10)
  : __ENV.MAX_VUS
    ? parseInt(__ENV.MAX_VUS, 10)
    : 30;
const gracefulStop = __ENV[`${config.envPrefix}_GRACEFUL_STOP`] || __ENV.GRACEFUL_STOP || '15m';

validateVuAllocation(__ENV, config.envPrefix, stages, config.maxObservationSeconds, maxVUs);

// ---------------------------------------------------------------------------
// k6 Options
// ---------------------------------------------------------------------------
export const options = {
  scenarios: {
    pipeline_load: {
      executor: 'ramping-arrival-rate',
      startRate,
      timeUnit: '1m',
      preAllocatedVUs,
      maxVUs,
      gracefulStop,
      stages,
    },
  },
  thresholds: {
    // Arrival schedule validity check: dropped_iterations must be strictly
    // 0 for the window to be treated as "arrival matched target" (see
    // docs/load-test-plan-event-sourcing.md section 4). Not abortOnFail, so
    // a breach is visible in the summary without killing VUs mid-drain.
    dropped_iterations: ['count==0'],
  },
};

// ---------------------------------------------------------------------------
// Redis client (one per VU, finite command timeouts from config)
// ---------------------------------------------------------------------------
const redisClient = new redis.Client(
  parseRedisConfig(config.redisUrl, {
    dialTimeout: config.redisDialTimeout,
    readTimeout: config.redisReadTimeout,
    writeTimeout: config.redisWriteTimeout,
  })
);

function generateUUID() {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function logRecord(record) {
  console.log(JSON.stringify(record));
}

function baseTags(dataset) {
  return { workload: config.workload, size_class: dataset.size_class, format: dataset.format };
}

// ---------------------------------------------------------------------------
// Admission guardrail
//
// Outstanding is tracked as "accepted publishes not yet observed terminal
// BY THIS SCRIPT", via a run-scoped Redis counter — not the number of VUs
// currently polling. It intentionally is NOT decremented on
// OBSERVATION_TIMEOUT (the backend may still complete the request after our
// observer gives up), so a run with many timeouts can permanently inflate
// this counter; that is a known, documented limitation (see k6/README.md),
// not a bug. stop_reason lets an operator or the resource-monitor tool
// (loadtest/tools/resources) halt new admissions on an OOM/worker-death
// signal without touching k6 itself:
//   redis-cli -u $REDIS_URL SET loadtest:<run_id>:stop_reason "OOM_DETECTED"
// ---------------------------------------------------------------------------
function outstandingKey(runId) {
  return `${config.guardrailPrefix}${runId}:outstanding`;
}
function stopReasonKey(runId) {
  return `${config.guardrailPrefix}${runId}:stop_reason`;
}
function admissionStoppedAtKey(runId) {
  return `${config.guardrailPrefix}${runId}:admission_stopped_at`;
}

async function checkAdmissionGate(runId) {
  let stopReason = null;
  try {
    stopReason = await redisClient.sendCommand('GET', stopReasonKey(runId));
  } catch (e) {
    // Best-effort: a transient Redis error here must not itself stop
    // admission, since that would make the guardrail less available than
    // the system it's protecting.
  }
  if (stopReason) {
    await markAdmissionStopped(runId);
    return { stopped: true, reason: `EXTERNAL:${stopReason}` };
  }

  let outstanding = 0;
  try {
    const v = await redisClient.sendCommand('GET', outstandingKey(runId));
    outstanding = v ? parseInt(v, 10) : 0;
  } catch (e) {
    // Best-effort; treat as 0 rather than blocking admission on a read blip.
  }
  if (outstanding >= config.outstandingLimit) {
    await markAdmissionStopped(runId);
    return { stopped: true, reason: `OUTSTANDING_LIMIT:${outstanding}` };
  }
  return { stopped: false };
}

async function markAdmissionStopped(runId) {
  try {
    await redisClient.sendCommand('SET', admissionStoppedAtKey(runId), String(Date.now()), 'NX');
  } catch (e) {
    // Non-fatal: the ADMISSION_STOPPED log lines are still authoritative
    // evidence of the stop window even if this marker write fails.
  }
}

async function incrOutstanding(runId) {
  try {
    await redisClient.sendCommand('INCR', outstandingKey(runId));
  } catch (e) {
    console.warn(JSON.stringify({ event: 'GUARDRAIL_INCR_FAILED', run_id: runId, error: String(e) }));
  }
}

async function decrOutstanding(runId) {
  try {
    await redisClient.sendCommand('DECR', outstandingKey(runId));
  } catch (e) {
    console.warn(JSON.stringify({ event: 'GUARDRAIL_DECR_FAILED', run_id: runId, error: String(e) }));
  }
}

// ---------------------------------------------------------------------------
// Lifecycle: Setup & Teardown
//
// RUN_ID is generated exactly once here (or taken from the env), and passed
// to every VU via the `data` argument of default(). It must never be
// computed at module top-level, since k6 re-evaluates top-level code
// independently in every VU's JS runtime: a top-level `Date.now()` fallback
// would silently give each VU a different run_id.
// ---------------------------------------------------------------------------
export function setup() {
  const runId = __ENV.RUN_ID || `run-${Date.now()}`;

  const summary = {
    timestamp: new Date().toISOString(),
    event: 'RUN_START',
    run_id: runId,
    workload: config.workload,
    manifest_path: config.manifestPath,
    allow_unverified_fixtures: config.allowUnverifiedFixtures,
    fixture_ids_pin: config.fixtureIds,
    mix_small_ratio: config.workload === 'mixed' ? config.mixSmallRatio : undefined,
    mixed_order: config.workload === 'mixed' ? config.mixedOrder : undefined,
    mixed_block_size: config.workload === 'mixed' && config.mixedOrder === 'block' ? config.mixedBlockSize : undefined,
    workload_seed: config.seed,
    small_pool_order: smallOrder ? smallOrder.map((f) => f.id) : undefined,
    large_pool_order: largeOrder ? largeOrder.map((f) => f.id) : undefined,
    outstanding_limit: config.outstandingLimit,
    redis_url: redactUrl(config.redisUrl),
    input_stream: config.inputStream,
    state_prefix: config.stateKeyPrefix,
    poll_interval_ms: config.pollIntervalMs,
    max_observation_seconds: config.maxObservationSeconds,
    stages,
    start_rate: startRate,
    pre_allocated_vus: preAllocatedVUs,
    max_vus: maxVUs,
    graceful_stop: gracefulStop,
    message:
      'k6 load-test started. Authoritative throughput/accepted reconciliation requires ' +
      'loadtest/tools/drain run after this run drains. Run `loadtest/tools/drain record` ' +
      'BEFORE this script if you have not already, to capture stream start IDs.',
  };
  console.log(JSON.stringify(summary));
  return { runId };
}

export function teardown(data) {
  console.log(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      event: 'RUN_END',
      run_id: data ? data.runId : null,
      finished_at: Date.now(),
      message:
        'k6 arrival + gracefulStop window complete. This does NOT prove the backend has drained: ' +
        'run `loadtest/tools/drain run` and wait for it to report queue/PEL at baseline or its own deadline.',
    })
  );
}

// ---------------------------------------------------------------------------
// Main VU Iteration Function
// ---------------------------------------------------------------------------
export default async function (data) {
  const runId = data.runId;
  const iterIndex = exec.scenario.iterationInTest;
  const vuId = exec.vu.idInTest;

  // 0. Admission guardrail: checked BEFORE any dataset selection or intent
  // logging, so a stopped run never even picks a fixture for this slot.
  // This does not cancel or affect VUs already polling an accepted request;
  // only NEW admissions stop.
  const gate = await checkAdmissionGate(runId);
  if (gate.stopped) {
    admissionStopped.add(1, { workload: config.workload });
    logRecord({
      timestamp: new Date().toISOString(),
      event: 'ADMISSION_STOPPED',
      run_id: runId,
      iteration: iterIndex,
      vu: vuId,
      reason: gate.reason,
    });
    return;
  }

  // 1. Deterministic fixture selection for this global iteration slot.
  const dataset = datasetForIteration(iterIndex);
  const tags = baseTags(dataset);

  // 2. RFC 4122 v4 UUIDs for request/conversation/message identities.
  const requestId = generateUUID();
  const conversationId = generateUUID();
  const messageId = generateUUID();

  const payload = {
    request_id: requestId,
    conversation_id: conversationId,
    message_id: messageId,
    file_path: dataset.url,
  };
  const payloadStr = JSON.stringify(payload);

  // 3. Log intent BEFORE XADD: if the process crashes or the publish reply
  // is lost, this line is still evidence the request was attempted, so the
  // drain tool can look it up directly in the input stream by request_id.
  const intentTimeMs = Date.now();
  logRecord({
    timestamp: new Date().toISOString(),
    event: 'INTENT',
    run_id: runId,
    iteration: iterIndex,
    vu: vuId,
    request_id: requestId,
    conversation_id: conversationId,
    message_id: messageId,
    fixture_id: dataset.id,
    dataset: { format: dataset.format, size_class: dataset.size_class, url: dataset.url, bytes: dataset.bytes },
    intent_time_ms: intentTimeMs,
  });

  let streamId = null;
  let publishError = null;
  try {
    streamId = await redisClient.sendCommand('XADD', config.inputStream, '*', 'data', payloadStr);
  } catch (err) {
    publishError = err && err.message ? err.message : String(err);
  }
  const publishReplyTimeMs = Date.now();

  if (publishError !== null || !streamId) {
    publishErrors.add(1, tags);
    logRecord({
      timestamp: new Date().toISOString(),
      event: 'PUBLISH_UNKNOWN',
      run_id: runId,
      iteration: iterIndex,
      vu: vuId,
      request_id: requestId,
      conversation_id: conversationId,
      message_id: messageId,
      fixture_id: dataset.id,
      dataset: { format: dataset.format, size_class: dataset.size_class, url: dataset.url, bytes: dataset.bytes },
      publish_duration_ms: publishReplyTimeMs - intentTimeMs,
      error: publishError || 'Empty stream ID returned',
      outcome: 'PUBLISH_UNKNOWN',
      note: 'Reply lost or failed; Redis may still have accepted the XADD. Not retried as a new request_id. Reconcile against the input stream in loadtest/tools/drain.',
    });
    return;
  }

  acceptedPublishes.add(1, tags);
  await incrOutstanding(runId);
  logRecord({
    timestamp: new Date().toISOString(),
    event: 'ACCEPTED',
    run_id: runId,
    iteration: iterIndex,
    vu: vuId,
    request_id: requestId,
    fixture_id: dataset.id,
    stream_id: streamId,
    publish_duration_ms: publishReplyTimeMs - intentTimeMs,
  });

  // 4. Poll the state hash every POLL_INTERVAL_MS using an elapsed-time
  // deadline (never a fixed loop-count, since each Redis call also
  // consumes wall-clock time against the deadline).
  const deadline = Date.now() + config.maxObservationSeconds * 1000;
  const stateKey = `${config.stateKeyPrefix}${requestId}`;

  let pollCount = 0;
  let readErrorCount = 0;
  let terminalStatus = null; // 'COMPLETED' | 'FAILED'
  let lastObservedStatus = null;

  while (Date.now() < deadline) {
    pollCount++;
    try {
      const status = await redisClient.sendCommand('HGET', stateKey, 'status');
      if (status) {
        lastObservedStatus = status;
        if (status === 'COMPLETED' || status === 'FAILED') {
          terminalStatus = status;
          break;
        }
      }
    } catch (err) {
      readErrorCount++;
      redisReadErrors.add(1, tags);
      // Transient read error: retry within the same deadline.
    }

    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) {
      break;
    }
    sleep(Math.min(config.pollIntervalMs, remainingMs) / 1000);
  }

  // 5. Outcome evaluation.
  const now = Date.now();
  let outcome;
  let e2eMs = null;

  if (terminalStatus === 'COMPLETED') {
    outcome = 'COMPLETED';
    e2eMs = now - intentTimeMs;
    observedSuccesses.add(1, tags);
    e2eDuration.add(e2eMs, tags);
    await decrOutstanding(runId);
  } else if (terminalStatus === 'FAILED') {
    outcome = 'FAILED';
    e2eMs = now - intentTimeMs;
    terminalFailures.add(1, tags);
    failedDuration.add(e2eMs, tags);
    await decrOutstanding(runId);
  } else {
    // Deadline reached without a terminal state. The task is NOT cancelled
    // on the backend, and outstanding is intentionally left incremented:
    // see the guardrail comment above.
    outcome = 'OBSERVATION_TIMEOUT';
    observationTimeouts.add(1, tags);
  }

  // 6. Final structured record with full correlation context.
  logRecord({
    timestamp: new Date().toISOString(),
    event: 'OUTCOME',
    run_id: runId,
    iteration: iterIndex,
    vu: vuId,
    request_id: requestId,
    conversation_id: conversationId,
    message_id: messageId,
    fixture_id: dataset.id,
    dataset: { format: dataset.format, size_class: dataset.size_class, url: dataset.url, bytes: dataset.bytes },
    stream_id: streamId,
    polling: {
      poll_count: pollCount,
      read_errors: readErrorCount,
      last_observed_status: lastObservedStatus,
      e2e_duration_ms: e2eMs,
    },
    outcome,
  });
}
