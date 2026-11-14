# go-markitdown

Go gRPC server with one REST endpoint for health checks. No conversion endpoint is implemented yet.

- REST: `GET /api/status` returns HTTP 200 with `{"status":"SERVING"}` when the gRPC health service is serving, or HTTP 503 with `{"status":"NOT_SERVING"}` otherwise.
- gRPC: standard `grpc.health.v1.Health` service (empty service name for overall health).
- Other HTTP paths return 404; other methods on `/api/status` return 405.
- SIGINT and SIGTERM trigger graceful shutdown with a five-second deadline.

## Run locally

Requires Go 1.26 or later:

```sh
go run ./cmd/markitdown-server
curl -i http://localhost:8080/api/status
```

Configure listening addresses using `HTTP_ADDR` (default `:8080`) and `GRPC_ADDR` (default `:9090`). The servers use plaintext connections; terminate TLS at your ingress when needed.

## Docker

Run these commands from the repository root:

```sh
docker build -f deploy/markitdown-server.dockerfile -t markitdown-server .
docker run -d --name markitdown-server -p 8080:8080 -p 9090:9090 markitdown-server
curl -i http://localhost:8080/api/status
docker inspect --format '{{.State.Health.Status}}' markitdown-server
```

The image runs as a non-root user and includes Python 3.12, FFmpeg, and `markitdown[all]` installed directly from the [official Microsoft source](https://github.com/microsoft/markitdown), pinned to the v0.1.9 source commit. Override the source ref when building:

```sh
docker build -f deploy/markitdown-server.dockerfile \
  --build-arg MARKITDOWN_REF=21d9c1d01ecc12538dab19811fd247ef2f96ef60 \
  -t markitdown-server .
```

When changing container ports, set the corresponding address variable and port mapping together. The Docker health check follows `HTTP_ADDR`. MarkItDown is installed for future conversion work; health checks verify the running gRPC service, not document conversion or external providers.
