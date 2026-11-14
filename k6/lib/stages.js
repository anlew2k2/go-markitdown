// stages.js
//
// Per-workload ramping-arrival-rate stage schedules and VU-allocation
// validation. See docs/load-test-plan-event-sourcing.md section 3 for the
// rationale: baseline is a hold with no ramp, every level after it gets its
// own {ramp, hold} pair at the SAME target (never a ramp straight down to
// 0, which would still emit arrivals while "stopping").

// buildStages reads <PREFIX>_RATE_LEVELS / <PREFIX>_STAGE_HOLD_DURATION /
// <PREFIX>_STAGE_RAMP_DURATION (falling back to the unprefixed env var, then
// a default), or a full <PREFIX>_STAGES_JSON / STAGES_JSON override.
export function buildStages(env, prefix) {
  const stagesJson = env[`${prefix}_STAGES_JSON`] || env.STAGES_JSON;
  if (stagesJson) {
    let parsed;
    try {
      parsed = JSON.parse(stagesJson);
    } catch (e) {
      throw new Error(`${prefix}_STAGES_JSON / STAGES_JSON is not valid JSON: ${e.message}`);
    }
    if (!Array.isArray(parsed) || parsed.length === 0) {
      throw new Error(`${prefix}_STAGES_JSON / STAGES_JSON must be a non-empty array of {target, duration}`);
    }
    return parsed;
  }

  const levelsRaw = env[`${prefix}_RATE_LEVELS`] || env.RATE_LEVELS || '1,2,3,4,5';
  const levels = levelsRaw.split(',').map((s) => {
    const n = parseInt(s.trim(), 10);
    if (!Number.isFinite(n) || n <= 0) {
      throw new Error(`${prefix}_RATE_LEVELS / RATE_LEVELS has a non-positive-integer entry: "${s}"`);
    }
    return n;
  });

  const hold = env[`${prefix}_STAGE_HOLD_DURATION`] || env.STAGE_HOLD_DURATION || '10m';
  const ramp = env[`${prefix}_STAGE_RAMP_DURATION`] || env.STAGE_RAMP_DURATION || '30s';

  const stages = [];
  levels.forEach((target, idx) => {
    if (idx === 0) {
      stages.push({ target, duration: hold }); // baseline: hold only, no ramp
    } else {
      stages.push({ target, duration: ramp }); // ramp up to this level
      stages.push({ target, duration: hold }); // hold at this level
    }
  });
  return stages;
}

export function startRateOf(stages) {
  return stages[0].target;
}

export function maxTargetOf(stages) {
  return stages.reduce((max, s) => Math.max(max, s.target), 0);
}

// validateVuAllocation enforces the guidance in load-test-review:
// with V VUs polling for up to maxObservationSeconds, the executor needs
// roughly rate * (observationSeconds / 60) VUs just to hold open requests
// still waiting on a terminal state, plus headroom for jitter/retries.
// Set SKIP_VU_CHECK=true to intentionally run under-provisioned (e.g. to
// exercise the outstanding-request guardrail itself).
export function validateVuAllocation(env, prefix, stages, maxObservationSeconds, maxVUs) {
  if (env.SKIP_VU_CHECK === 'true') {
    return;
  }
  const headroom = env.VU_HEADROOM_FACTOR ? parseFloat(env.VU_HEADROOM_FACTOR) : 1.2;
  const maxTarget = maxTargetOf(stages);
  const required = Math.ceil(maxTarget * (maxObservationSeconds / 60) * headroom);
  if (maxVUs < required) {
    throw new Error(
      `[${prefix}] MAX_VUS=${maxVUs} is likely insufficient for max arrival rate ${maxTarget}/min with ` +
        `MAX_OBSERVATION_SECONDS=${maxObservationSeconds}s: need >= ${required} ` +
        `(rate x wait-time x ${headroom} headroom). Raise MAX_VUS/PRE_ALLOCATED_VUS to >= ${required}, ` +
        'or set SKIP_VU_CHECK=true to proceed anyway and expect dropped_iterations > 0.'
    );
  }
}
