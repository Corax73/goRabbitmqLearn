package learn

import (
	"context"
	"log"
	"sync"

	goutils "github.com/Corax73/goUtils"
	"github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	QueueTitle string
	Rmq        *MyRabbitmq
}

func (worker *Worker) Run(callback func(worker *Worker, msg amqp091.Delivery)) {
	log.Printf("consumer for queue `%s` started", worker.QueueTitle)
	msgs, err := worker.Rmq.Consume(context.Background(), worker.QueueTitle)
	if err != nil {
		goutils.Logging(err)
	} else {
		var wg sync.WaitGroup
		wg.Go(func() {
			for d := range msgs {
				callback(worker, d)
				err := d.Ack(false)
				if err != nil {
					goutils.Logging(err)
				}
			}
		})
	}
}
