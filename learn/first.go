package learn

import (
	"context"
	"fmt"
	"sync"
	"time"

	goutils "github.com/Corax73/goUtils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnectConfig struct {
	Login, Password, Ip, Port, QueueTitle, ExchangeTitle, ExchangeType, RoutingKey, ConsumerTitle string
	Durable, AutoDelete, Exclusive, NoWait, Internal, Mandatory, Immediate, AutoAck, NoLocal      bool
	Args                                                                                          amqp.Table
}

type MyRabbitmq struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   *amqp.Queue
	mu      sync.Mutex
	config  *ConnectConfig
}

func Init(config *ConnectConfig) *MyRabbitmq {
	r := &MyRabbitmq{config: config}
	r.setConnect()
	r.setChannel()
	r.setQueue()
	r.setExchange()
	return r
}

func (r *MyRabbitmq) setConnect() {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", r.config.Login, r.config.Password, r.config.Ip, r.config.Port))
	if err != nil {
		goutils.Logging(err)
	}
	r.conn = conn
}

func (r *MyRabbitmq) setChannel() {
	ch, err := r.conn.Channel()
	if err != nil {
		goutils.Logging(err)
	}
	r.channel = ch
}

func (r *MyRabbitmq) setQueue() {
	q, err := r.channel.QueueDeclare(
		r.config.QueueTitle,
		r.config.Durable,
		r.config.AutoDelete,
		r.config.Exclusive,
		r.config.NoWait,
		r.config.Args,
	)
	if err != nil {
		goutils.Logging(err)
	}
	r.queue = &q
}

func (r *MyRabbitmq) setExchange() {
	err := r.channel.ExchangeDeclare(
		r.config.ExchangeTitle,
		r.config.ExchangeType,
		r.config.Durable,
		r.config.AutoDelete,
		r.config.Internal,
		r.config.NoWait,
		r.config.Args,
	)
	if err != nil {
		goutils.Logging(err)
	}
	err = r.channel.QueueBind(
		r.config.QueueTitle,
		r.config.RoutingKey,
		r.config.ExchangeTitle,
		r.config.NoWait,
		r.config.Args,
	)
	if err != nil {
		goutils.Logging(err)
	}
}

func (r *MyRabbitmq) Publish(message string) bool {
	err := r.channel.Publish(
		r.config.ExchangeTitle,
		r.config.RoutingKey,
		r.config.Mandatory,
		r.config.Immediate,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(message),
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		goutils.Logging(err)
		return false
	} else {
		return true
	}
}

func (r *MyRabbitmq) Consume(ctx context.Context, qName string) (<-chan amqp.Delivery, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel == nil {
		return nil, fmt.Errorf("conn is nil")
	}
	msgs, err := r.channel.Consume(
		r.config.QueueTitle,
		r.config.ConsumerTitle,
		r.config.AutoAck,
		r.config.Exclusive,
		r.config.NoLocal,
		r.config.NoWait,
		r.config.Args,
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
