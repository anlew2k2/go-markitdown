#!/usr/bin/env python3
"""Persistent MarkItDown conversion worker.

Run as `python3 -u markitdown_worker.py` by
internal/services/persistent_converter.go (CONVERTER_BACKEND=persistent).
Imports and constructs MarkItDown exactly once, then serves conversion
requests for local files over a tiny JSON-Lines protocol on stdin/stdout,
so the Go extractor does not pay Python/MarkItDown startup and import cost
on every single part (as it does with the `markitdown` CLI).

Protocol (newline-delimited JSON, one object per line):
  stdout, once at startup, after import+init succeed:
    {"type": "ready", "init_ms": <int>}
  (on init failure: nothing is written to stdout; the process logs to
  stderr and exits non-zero — the Go side treats "process exited before any
  ready line" as a startup failure either way.)

  stdin, one line per request:
    {"id": "<string>", "input_path": "<local path>", "output_path": "<local path>"}

  stdout, exactly one line per request received, after output_path has been
  written and closed (or the conversion failed):
    {"id": "<same string>", "ok": true, "convert_ms": <int>}
    {"id": "<same string>", "ok": false, "error": "<bounded string>", "convert_ms": <int>}

Only paths and small scalars cross the protocol — never file contents, so a
request/response line is always small; stdin/stdout are both the control
channel and are never used for file content.

stdout is reserved for the protocol above. Before importing MarkItDown (or
doing anything else that might print), stdout is swapped for stderr so any
incidental library output can never corrupt the protocol stream; the real
stdout handle is kept for _send() alone.
"""
import json
import sys
import time

_protocol_out = sys.stdout
sys.stdout = sys.stderr

MAX_LINE_BYTES = 1_000_000
MAX_ERROR_CHARS = 2000


def _send(obj):
    _protocol_out.write(json.dumps(obj) + "\n")
    _protocol_out.flush()


def _bounded(text):
    text = str(text)
    if len(text) > MAX_ERROR_CHARS:
        return text[:MAX_ERROR_CHARS]
    return text


def main():
    start = time.monotonic()
    try:
        from markitdown import MarkItDown

        converter = MarkItDown()
    except Exception as exc:  # init failure: no ready line, exit non-zero
        print(f"markitdown_worker: init failed: {exc}", file=sys.stderr)
        sys.exit(1)

    _send({"type": "ready", "init_ms": int((time.monotonic() - start) * 1000)})

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        if len(raw) > MAX_LINE_BYTES:
            _send({"id": None, "ok": False, "error": "request line exceeds size limit"})
            continue

        try:
            req = json.loads(raw)
            req_id = req["id"]
            input_path = req["input_path"]
            output_path = req["output_path"]
        except Exception as exc:
            _send({"id": None, "ok": False, "error": _bounded(f"invalid request: {exc}")})
            continue

        job_start = time.monotonic()
        try:
            result = converter.convert(input_path)
            # Match the markitdown CLI's own output exactly: it writes
            # text_content via `print(...)`, which appends a trailing
            # newline markitdown_worker.go's CLIConverter path would
            # otherwise differ from part output content.
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(result.text_content)
                f.write("\n")
            _send({
                "id": req_id,
                "ok": True,
                "convert_ms": int((time.monotonic() - job_start) * 1000),
            })
        except Exception as exc:
            _send({
                "id": req_id,
                "ok": False,
                "error": _bounded(f"{type(exc).__name__}: {exc}"),
                "convert_ms": int((time.monotonic() - job_start) * 1000),
            })


if __name__ == "__main__":
    main()
