.PHONY: build execute new-miner clean setup test

BIN_NAME=tcp_luxor

build:
	@mkdir -p bin
	@go build -o bin/${BIN_NAME} cmd/server/main.go

execute: setup build
	@./bin/${BIN_NAME}

new-miner:
	@go run cmd/client/main.go $(name)

setup:
	@docker compose up -d

test:
	@go test ./... --race

clean:
	@docker compose down -v
	@rm -rf bin