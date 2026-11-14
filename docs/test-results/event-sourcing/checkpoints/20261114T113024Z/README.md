# Interim load-test checkpoint

Captured at 2026-11-14T11:30:24.079638+00:00. This is an immutable snapshot of an ongoing run, not a final capacity or acceptance verdict.

Run: `run-1794655173-small`; workload: small. agy and k6 were running at capture. Configured rates: 5, 10, 15, 20, 25 requests/minute (0.083–0.417 requests/second), with 10-minute holds and 30-second ramps; nominal scenario duration 52 minutes. Observation timeout: 120 seconds.

## Results so far

- Event window: 618.4 seconds.
- Event counts: {'RUN_START': 1, 'INTENT': 52, 'ACCEPTED': 52, 'OUTCOME': 49}.
- Provisional k6 outcomes: {'COMPLETED': 43, 'OBSERVATION_TIMEOUT': 6}.
- Completed throughput over the event window: 0.0695 requests/second. This includes startup and is not maximum sustainable capacity.
- Observation timeouts do not establish backend failure. Final classification requires output-stream reconciliation/drain, which is absent from this snapshot.

| Fixture | Provisional outcomes |
| --- | --- |
| csv-boundary-under-16mib | {'COMPLETED': 6} |
| docx-small-100kb | {'COMPLETED': 6} |
| docx-small-1mib | {'COMPLETED': 6} |
| pdf-small-1mib | {'COMPLETED': 6} |
| pdf-small-240kb | {'COMPLETED': 6} |
| pdf-small-8mib | {'OBSERVATION_TIMEOUT': 6} |
| xlsx-small-100kb | {'COMPLETED': 6} |
| xlsx-small-1mib | {'COMPLETED': 7} |

## Evidence limitations

- Raw resource stats contain ANSI escape sequences causing collector parse errors; raw lines are preserved. Do not treat missing parsed metrics as zero.
- Splitter cgroup collection reports `status: ok` despite MISSING values/path errors. Splitter cgroup metrics are unavailable; this is a monitoring gap.
- Extractor cgroup counters are cumulative; absolute peaks/counters cannot be attributed exclusively to this run without baseline comparison.
- No final drain report or large/mixed workload results are present in this run snapshot.
- No timeout, split threshold, worker count, or application code was changed for this checkpoint.

## Preserved evidence

`evidence/` contains copies of the live k6 log, stream-start manifest, resource stats, cgroup samples, and container event log at capture. `summary.json` contains the aggregated event counts. Original live files remain under `../run-002/`.
