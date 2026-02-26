package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"tcp_luxor/infra/db"
	"tcp_luxor/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())

	conn, err := db.New(ctx)
	if err != nil {
		log.Fatalf("error connecting to database: %s", err)
	}

	server := server.NewServer("12345", conn)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

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
