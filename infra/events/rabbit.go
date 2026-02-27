package events

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	submissionsQueue = "submissions"
	publisherBuffer  = 1000
)

// SubmissionEvent is the message published to RabbitMQ on every valid submission.
// The consumer reads these events and persists them on PostgreSQL.
type SubmissionEvent struct {
	Username  string    `json:"username"`
	Timestamp time.Time `json:"timestamp"`
}

func dial(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("rabbit: failed to connect: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("rabbit: failed to open channel: %w", err)
	}

	if _, err = ch.QueueDeclare(
		submissionsQueue,
		true,  // durable
		false, // auto delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("rabbit: failed to declare queue: %w", err)
	}

	return conn, ch, nil
}
