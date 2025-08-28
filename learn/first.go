package learn

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MyRabbitmq struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Mu      sync.Mutex
}

func (r *MyRabbitmq) Consume(ctx context.Context, qName string) (<-chan amqp.Delivery, error) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

	if r.Channel == nil {
		return nil, fmt.Errorf("conn is nil")
	}
	msgs, err := r.Channel.Consume(
		qName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("err: %w", err)
	}

	out := make(chan amqp.Delivery)
	go func() {
		defer close(out)
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					return
				}
				out <- d
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}