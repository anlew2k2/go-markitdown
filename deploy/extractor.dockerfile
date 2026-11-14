# syntax=docker/dockerfile:1

# ---- step 1: markitdown base image (cached unless this stage itself changes) ----
FROM python:3.13-slim AS markitdown-base
RUN pip install --no-cache-dir "markitdown[all]"

# ---- step 2: build go server ----
FROM golang:1.27-bookworm AS deps
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

FROM deps AS builder
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /out/extractor ./cmd/extractor

# ---- step 3: copy go binary into the markitdown base image and run ----
FROM markitdown-base AS runtime
WORKDIR /app
COPY --from=builder /out/extractor ./extractor

EXPOSE 8080
ENTRYPOINT ["./extractor"]
