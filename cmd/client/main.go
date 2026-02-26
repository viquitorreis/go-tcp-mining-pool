package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"tcp_luxor/miner"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	minerName := "miner1"
	if len(os.Args) > 1 {
		name := os.Args[1]
		if strings.TrimSpace(name) == "" {
			slog.Warn("miner name cannot be empty setting to default.", "name", minerName)
		} else {
			minerName = name
		}
	}

	slog.Info("miner", "name", minerName)

	m := miner.New("localhost:12345", minerName)
	if err := m.Run(ctx); err != nil {
		slog.Error("miner error", "error", err)
		os.Exit(1)
	}
}
