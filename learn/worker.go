package learn

import (
	"context"
	"fmt"
	"sync"

	goutils "github.com/Corax73/goUtils"
	"github.com/rabbitmq/amqp091-go"
)

type Worker struct{
	QueueTitle string
	Rmq *MyRabbitmq
}

func (worker *Worker) Run() {
	fmt.Println("worker.QueueTitle", worker.QueueTitle)
	msgs, err := worker.Rmq.Consume(context.Background(), worker.QueueTitle)
	if err != nil {
		goutils.Logging(err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for d := range msgs {
			worker.Handle(d)
			err := d.Ack(false)
			if err != nil {
				goutils.Logging(err)
			}
		}
	})
}

func (worker *Worker) Handle(msg amqp091.Delivery) {
	fmt.Println(worker.Rmq.GetConfig().ConsumerTitle, string(msg.Body))
}