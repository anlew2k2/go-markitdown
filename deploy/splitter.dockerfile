# syntax=docker/dockerfile:1

# ---- step 1: build go server ----
FROM golang:1.27-bookworm AS deps
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

FROM deps AS builder
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /out/splitter ./cmd/splitter

# ---- step 2: copy go binary into a minimal runtime image ----
FROM gcr.io/distroless/static-debian12 AS runtime
WORKDIR /app
COPY --from=builder /out/splitter ./splitter

EXPOSE 8080
ENTRYPOINT ["./splitter"]
