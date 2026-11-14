import { sleep } from 'k6';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';
import redis from 'k6/x/redis';

// ---------------------------------------------------------------------------
// Custom Metrics
// ---------------------------------------------------------------------------
const acceptedPublishes = new Counter('accepted_publishes');
const publishErrors = new Counter('publish_errors');
const observedSuccesses = new Counter('observed_successes');
const terminalFailures = new Counter('terminal_failures');
const observationTimeouts = new Counter('observation_timeouts');
const redisReadErrors = new Counter('redis_read_errors');
const e2eDuration = new Trend('e2e_duration', true);

// ---------------------------------------------------------------------------
// Stage Schedule Builder
// ---------------------------------------------------------------------------
// 1. Ramping-arrival-rate with short ramps and explicit 10-minute plateaus at 1-5 docs/min.
// 2. Stop new arrivals completely during drain (no ramp-down stage to 0).
// 3. GracefulStop gives in-flight iterations up to 15m to finish observation.
function buildStages() {
  if (__ENV.STAGES_JSON) {
    try {
      return JSON.parse(__ENV.STAGES_JSON);
    } catch (e) {
      console.warn(`Failed to parse STAGES_JSON: ${e.message}. Using default schedule.`);
    }
  }

  const hold = __ENV.STAGE_HOLD_DURATION || '10m';
  const ramp = __ENV.STAGE_RAMP_DURATION || '30s';

  return [
    { target: 1, duration: hold }, // Baseline plateau: 1 doc/min
    { target: 2, duration: ramp }, // Ramp to 2 docs/min
    { target: 2, duration: hold }, // Plateau: 2 docs/min
    { target: 3, duration: ramp }, // Ramp to 3 docs/min
    { target: 3, duration: hold }, // Plateau: 3 docs/min
    { target: 4, duration: ramp }, // Ramp to 4 docs/min
    { target: 4, duration: hold }, // Plateau: 4 docs/min
    { target: 5, duration: ramp }, // Ramp to 5 docs/min
    { target: 5, duration: hold }, // Plateau: 5 docs/min
  ];
}

// ---------------------------------------------------------------------------
// k6 Options & Thresholds
// ---------------------------------------------------------------------------
export const options = {
  scenarios: {
    pipeline_load: {
      executor: 'ramping-arrival-rate',
      startRate: __ENV.START_RATE ? parseInt(__ENV.START_RATE, 10) : 1,
      timeUnit: '1m',
      preAllocatedVUs: __ENV.PRE_ALLOCATED_VUS ? parseInt(__ENV.PRE_ALLOCATED_VUS, 10) : 30,
      maxVUs: __ENV.MAX_VUS ? parseInt(__ENV.MAX_VUS, 10) : 30,
      gracefulStop: __ENV.GRACEFUL_STOP || '15m',
      stages: buildStages(),
    },
  },
  thresholds: {
    // Arrival schedule validity check: dropped iterations must be strictly 0
    dropped_iterations: ['count==0'],
  },
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
function parseRedisConfig(urlStr, timeouts = {}) {
  const match = (urlStr || '').match(/^rediss?:\/\/(?:([^:]*)(?::([^@]*))?@)?([^:\/?#]+)(?::([0-9]+))?(?:\/([0-9]+))?/);
  if (!match) {
    throw new Error(`Invalid Redis URL: ${urlStr}`);
  }
  const [, username, password, host, port, db] = match;
  const cfg = {
    socket: {
      host: host || '127.0.0.1',
      port: port ? parseInt(port, 10) : 6379,
      dialTimeout: timeouts.dialTimeout !== undefined ? timeouts.dialTimeout : 5,
      readTimeout: timeouts.readTimeout !== undefined ? timeouts.readTimeout : 5,
      writeTimeout: timeouts.writeTimeout !== undefined ? timeouts.writeTimeout : 5,
    },
    database: db ? parseInt(db, 10) : 0,
  };
  if (username) cfg.username = username;
  if (password) cfg.password = password;
  return cfg;
}

const config = {
  runId: __ENV.RUN_ID || `run-${Date.now()}`,
  redisUrl: __ENV.REDIS_URL || 'redis://127.0.0.1:6379',
  redisDialTimeout: __ENV.REDIS_DIAL_TIMEOUT ? parseInt(__ENV.REDIS_DIAL_TIMEOUT, 10) : 5,
  redisReadTimeout: __ENV.REDIS_READ_TIMEOUT ? parseInt(__ENV.REDIS_READ_TIMEOUT, 10) : 5,
  redisWriteTimeout: __ENV.REDIS_WRITE_TIMEOUT ? parseInt(__ENV.REDIS_WRITE_TIMEOUT, 10) : 5,
  inputStream: __ENV.INPUT_STREAM || 'stream:doc-extractor:input',
  stateKeyPrefix: __ENV.STATE_KEY_PREFIX || 'doc-extractor:state:',
  datasetPdfUrl: __ENV.DATASET_PDF_URL || 'http://minio:9000/dataset/sample_50mb.pdf',
  datasetDocxUrl: __ENV.DATASET_DOCX_URL || 'http://minio:9000/dataset/sample_50mb.docx',
  datasetXlsxUrl: __ENV.DATASET_XLSX_URL || 'http://minio:9000/dataset/sample_50mb.xlsx',
  datasetFileBytes: __ENV.DATASET_FILE_BYTES ? parseInt(__ENV.DATASET_FILE_BYTES, 10) : 52428800, // 50 MiB
  pollIntervalMs: __ENV.POLL_INTERVAL_MS ? parseInt(__ENV.POLL_INTERVAL_MS, 10) : 1000,
  maxObservationSeconds: __ENV.MAX_OBSERVATION_SECONDS ? parseInt(__ENV.MAX_OBSERVATION_SECONDS, 10) : 600, // 10 minutes
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
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

// Fixed rotation datasets: 1:1:1 mix
const datasets = [
  { format: 'pdf', url: config.datasetPdfUrl, bytes: config.datasetFileBytes },
  { format: 'docx', url: config.datasetDocxUrl, bytes: config.datasetFileBytes },
  { format: 'xlsx', url: config.datasetXlsxUrl, bytes: config.datasetFileBytes },
];

// Instantiated once per VU with finite command timeouts
const redisClient = new redis.Client(
  parseRedisConfig(config.redisUrl, {
    dialTimeout: config.redisDialTimeout,
    readTimeout: config.redisReadTimeout,
    writeTimeout: config.redisWriteTimeout,
  })
);

// ---------------------------------------------------------------------------
// Lifecycle: Setup & Teardown
// ---------------------------------------------------------------------------
export function setup() {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    event: 'SETUP',
    run_id: config.runId,
    redis_url: config.redisUrl,
    input_stream: config.inputStream,
    state_prefix: config.stateKeyPrefix,
    poll_interval_ms: config.pollIntervalMs,
    max_observation_seconds: config.maxObservationSeconds,
    message: 'k6 load-test scaffold initialized. Authoritative throughput will require output-stream reconciliation.',
  }));
  return { runId: config.runId, startedAt: Date.now() };
}

export function teardown(data) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    event: 'TEARDOWN',
    run_id: data ? data.runId : config.runId,
    finished_at: Date.now(),
    message: 'k6 load-test execution completed. In-flight iterations drained within gracefulStop.',
  }));
}

// ---------------------------------------------------------------------------
// Main VU Iteration Function
// ---------------------------------------------------------------------------
export default async function () {
  const iterIndex = exec.scenario.iterationInTest;
  const vuId = exec.vu.idInTest;

  // 1. Rotate dataset using scenario.iterationInTest (guarantees strict 1:1:1 mix across VUs)
  const dataset = datasets[iterIndex % datasets.length];

  // 2. Generate RFC 4122 v4 UUIDs for request, conversation, and message identities
  const requestId = generateUUID();
  const conversationId = generateUUID();
  const messageId = generateUUID();

  // 3. Document pipeline event contract payload
  const payload = {
    request_id: requestId,
    conversation_id: conversationId,
    message_id: messageId,
    file_path: dataset.url,
  };
  const payloadStr = JSON.stringify(payload);

  // 4. Publish to input stream (stream:doc-extractor:input) in 'data' field
  const publishStartTime = Date.now();
  let streamId = null;
  let publishError = null;

  try {
    streamId = await redisClient.sendCommand('XADD', config.inputStream, '*', 'data', payloadStr);
  } catch (err) {
    publishError = err && err.message ? err.message : String(err);
  }
  const publishReplyTime = Date.now();

  // If publish reply was lost or failed, record correlation details and return
  if (publishError !== null || !streamId) {
    publishErrors.add(1);
    const errorRecord = {
      timestamp: new Date().toISOString(),
      run_id: config.runId,
      iteration: iterIndex,
      vu: vuId,
      request_id: requestId,
      conversation_id: conversationId,
      message_id: messageId,
      dataset: {
        format: dataset.format,
        url: dataset.url,
        bytes: dataset.bytes,
      },
      publish: {
        start_time_ms: publishStartTime,
        reply_time_ms: publishReplyTime,
        duration_ms: publishReplyTime - publishStartTime,
        stream_id: null,
        error: publishError || 'Empty stream ID returned',
      },
      polling: {
        poll_count: 0,
        read_errors: 0,
        last_observed_status: null,
        e2e_duration_ms: null,
      },
      outcome: 'PUBLISH_UNKNOWN',
    };
    console.log(JSON.stringify(errorRecord));
    return;
  }

  acceptedPublishes.add(1);

  // 5. Poll request state (doc-extractor:state:<request_id>) every second using elapsed-time deadline
  const deadline = Date.now() + (config.maxObservationSeconds * 1000);
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
      redisReadErrors.add(1);
      // Transient read error: retry within deadline
    }

    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) {
      break;
    }
    const sleepMs = Math.min(config.pollIntervalMs, remainingMs);
    sleep(sleepMs / 1000);
  }

  // 6. Outcome evaluation
  const now = Date.now();
  let outcome;
  let e2eMs = null;

  if (terminalStatus === 'COMPLETED') {
    outcome = 'COMPLETED';
    e2eMs = now - publishStartTime;
    observedSuccesses.add(1);
    e2eDuration.add(e2eMs);
  } else if (terminalStatus === 'FAILED') {
    outcome = 'FAILED';
    e2eMs = now - publishStartTime;
    terminalFailures.add(1);
  } else {
    // Deadline reached without terminal state; task is NOT cancelled on backend
    outcome = 'OBSERVATION_TIMEOUT';
    observationTimeouts.add(1);
  }

  // 7. Emit structured log entry with full correlation context
  const summaryRecord = {
    timestamp: new Date().toISOString(),
    run_id: config.runId,
    iteration: iterIndex,
    vu: vuId,
    request_id: requestId,
    conversation_id: conversationId,
    message_id: messageId,
    dataset: {
      format: dataset.format,
      url: dataset.url,
      bytes: dataset.bytes,
    },
    publish: {
      start_time_ms: publishStartTime,
      reply_time_ms: publishReplyTime,
      duration_ms: publishReplyTime - publishStartTime,
      stream_id: streamId,
      error: null,
    },
    polling: {
      poll_count: pollCount,
      read_errors: readErrorCount,
      last_observed_status: lastObservedStatus,
      e2e_duration_ms: e2eMs,
    },
    outcome: outcome,
  };
  console.log(JSON.stringify(summaryRecord));
}
