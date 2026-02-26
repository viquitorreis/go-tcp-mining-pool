package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"tcp_luxor/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	server := server.NewServer("12345")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := server.Start(ctx); err != nil {
			log.Fatalf("err starting server: %s", err.Error())
		}
	}()

	go func() {
		<-sigChan
		cancel()
	}()

	<-ctx.Done()
	server.Stop()
}
