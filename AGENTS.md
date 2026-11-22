# Agent Instructions (`go-markitdown`)

## Scope & Boundaries
- Isolated document splitting & Markdown extraction workers (outside core SaaS stack).
- Must NOT expose public HTTP command execution (`subprocess/run`) or access core SaaS databases.
- Always fetch remote URLs through `internal/usecase/httpclient.go` (`doSafeRequest` with SSRF private-IP blocking, timeouts, and `io.LimitReader` bounds).

## Non-Negotiable AI Guardrails
1. Never commit real secrets/tokens to configs, `.env.example`, or tests.
2. Never add arbitrary shell/command execution endpoints.
3. Keep files focused and pin Python/Go dependencies.

## Tiered Verification
- **Tier 1:** `gofmt` + `go vet ./...` + `go test ./...`.
- **Tier 2/3 (SSRF/splitter/pipeline changes):** Tier 1 + targeted `go test` covering SSRF blocking, size limits, and worker state invariants.
