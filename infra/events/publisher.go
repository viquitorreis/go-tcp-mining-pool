package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher sends submissio nevents to RabbitMQ async
// It holds internal buffered channel so callers never block waiting for the broker
// If the buffer is full, the event is dropped and logged, this is a trade-off
// that keeps the TCP server responsive even if the RabbitMQ is slow
type Publisher struct {
	ch     *amqp.Channel
	conn   *amqp.Connection
	events chan SubmissionEvent
}

// NewPublisher connects to RabbitMQ and starts the background drain goroutine
func NewPublisher(url string) (*Publisher, error) {
	conn, ch, err := dial(url)
	if err != nil {
		return nil, err
	}

	p := &Publisher{
		ch:     ch,
		conn:   conn,
		events: make(chan SubmissionEvent, publisherBuffer),
	}

	return p, nil
}

// Run drains the internal channel and publishes each event to RabbitMQ
// it blocks until ctx is cancelled and then closes connection cleanly
func (p *Publisher) Run(ctx context.Context) {
	defer p.conn.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-p.events:
			if err := p.publish(event); err != nil {
				slog.Error("rabbit: failed to publish event", "error", err)
			}
		}
	}
}

func (p *Publisher) Publish(username string) {
	event := SubmissionEvent{
		Username:  username,
		Timestamp: time.Now(),
	}
	select {
	case p.events <- event:
		slog.Info("rabbit: event enqueued", "username", username)
	default:
		slog.Error("rabbit: publisher buffer full, dropping event", "username", username)
	}
}

func (p *Publisher) publish(event SubmissionEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.ch.Publish(
		"", // default exchange, will route directly to the q by name
		submissionsQueue,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // survives RabbitMQ restarts
			Body:         body,
		},
	)
}
