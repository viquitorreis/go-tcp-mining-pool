.PHONY: build execute

BIN_NAME=tcp_luxor

build:
	@mkdir -p bin
	@go build -o bin/${BIN_NAME} cmd/main.go

execute: build
	@./bin/${BIN_NAME}