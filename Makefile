.PHONY: build execute clean setup

BIN_NAME=tcp_luxor

build:
	@mkdir -p bin
	@go build -o bin/${BIN_NAME} cmd/main.go

execute: setup build
	@./bin/${BIN_NAME}

setup:
	@docker compose up -d

clean:
	@docker compose down -v
	@rm -rf bin