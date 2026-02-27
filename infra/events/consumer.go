package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"tcp_luxor/infra/db"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer reads submission events from RabbitMQ and persists them to PostgreSQL
// Each message is ACK only after a scuccesful database write, so no
// submission are lost even if the server restarts mid-processing
type Consumer struct {
	ch   *amqp.Channel
	conn *amqp.Connection
	db   *db.DB
}

// NewConsumer connects to RabbitMQ and returns a consumer ready to run
func NewConsumer(url string, db *db.DB) (*Consumer, error) {
	conn, ch, err := dial(url)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		ch:   ch,
		conn: conn,
		db:   db,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) {
	defer c.conn.Close()

	msgs, err := c.ch.Consume(
		submissionsQueue,
		"",
		false, // auto-ack false, we ack manually after successful db write
		false, // exclusive
		false, // no local
		false, // no wait
		nil,
	)
	if err != nil {
		slog.Error("rabbit: failed to start consuming", "error", err)
		return
	}

	slog.Info("rabbit: consumer started", "queue", submissionsQueue)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				slog.Warn("rabbit: channel closed, stopping consumer")
				return
			}

			c.handle(ctx, msg)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg amqp.Delivery) {
	var event SubmissionEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		slog.Error("rabbit: failed to unmarshal event, discarding", "error", err)
		// nack without requeue because its a bad message, will never parse correctly
		msg.Nack(false, false)
		return
	}

	model := db.SubmissionStatModel{
		Username:        event.Username,
		Timestamp:       event.Timestamp,
		SubmissionCount: 1,
	}

	slog.Info("consuming from q", "username", model.Username)

	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := c.db.UpsertSubmissions(
		ctxTimeout,
		[]db.SubmissionStatModel{model},
	); err != nil {
		slog.Error("rabbit: failed to persist event, requeuing", "error", err)
		// nack with requeue because its a transient db error, try again later...
		msg.Nack(false, true)
		return
	}

	msg.Ack(false)
}
