.PHONY: extractor\:build extractor\:run extractor\:docker\:build extractor\:docker\:run

extractor\:build:
	go build -o bin/extractor ./cmd/extractor

extractor\:run:
	go run ./cmd/extractor

extractor\:docker\:build:
	docker build -f deploy/extractor.dockerfile -t go-markitdown-extractor .

extractor\:docker\:run:
	docker run --rm -p 8080:8080 go-markitdown-extractor
