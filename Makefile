.PHONY: extractor\:build extractor\:run extractor\:docker\:build extractor\:docker\:run splitter\:build splitter\:run splitter\:docker\:build splitter\:docker\:run deploy\:up deploy\:down

extractor\:build:
	go build -o bin/extractor ./cmd/extractor

extractor\:run:
	go run ./cmd/extractor

extractor\:docker\:build:
	docker build -f deploy/extractor.dockerfile -t go-markitdown-extractor .

extractor\:docker\:run:
	docker run --rm -p 8080:8080 go-markitdown-extractor

splitter\:build:
	go build -o bin/splitter ./cmd/splitter

splitter\:run:
	go run ./cmd/splitter

splitter\:docker\:build:
	docker build -f deploy/splitter.dockerfile -t go-markitdown-splitter .

splitter\:docker\:run:
	docker run --rm -p 8081:8080 go-markitdown-splitter

deploy\:up:
	docker compose -f deploy/docker-compose.yaml up --build -d

deploy\:down:
	docker compose -f deploy/docker-compose.yaml down
