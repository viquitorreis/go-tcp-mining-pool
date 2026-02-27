package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tcp_luxor/infra/db"
	"tcp_luxor/infra/events"
	"tcp_luxor/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())

	conn, err := db.New(ctx)
	if err != nil {
		log.Fatalf("error connecting to database: %s", err)
	}

	const rabbitURL = "amqp://guest:guest@localhost:5672/"

	publisher, err := events.NewPublisher(rabbitURL)
	if err != nil {
		slog.Warn("RabbitMQ unavailable, running without async publishing", "error", err)
	} else {
		slog.Info("RabbitMQ publisher connected")
	}

	var consumer *events.Consumer
	if publisher != nil {
		consumer, err = events.NewConsumer(rabbitURL, conn)
		if err != nil {
			log.Fatalf("failed to create rabbit consumer: %s", err)
		}
	}

	server := server.NewServer("12345", conn, publisher)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if publisher != nil {
		go publisher.Run(ctx)
		go consumer.Run(ctx)
	}

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
