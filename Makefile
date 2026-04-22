.PHONY: build run test clean tidy

BINARY := bin/artifact-registry

build:
	mkdir -p bin
	go build -o $(BINARY) ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./...

tidy:
	go mod tidy

clean:
	rm -rf bin/ data/

# Quick smoke test after starting the server
smoke:
	@echo "--- health ---"
	curl -s http://localhost:8080/v1/health | jq .
	@echo "--- upload test artifact ---"
	echo "hello world" | curl -s -X POST http://localhost:8080/v1/artifacts/config/test \
	  -F "file=@-;filename=test.txt" \
	  -F "version=v1.0.0" | jq .
	@echo "--- promote to stable ---"
	curl -s -X POST http://localhost:8080/v1/channels/stable/promote/config/test/v1.0.0 | jq .
	@echo "--- resolve latest ---"
	curl -s "http://localhost:8080/v1/artifacts/config/test/latest?channel=stable" | jq .
