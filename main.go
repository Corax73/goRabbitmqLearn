package main

import (
	"context"
	"fmt"
	"rabbitmqlearn/learn"
	"strconv"
	"time"

	goutils "github.com/Corax73/goUtils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	goutils.LogInit("")
	conn := getConnect("ruser", "rpassword", "localhost", "5673")

	ch, err := conn.Channel()
	if err != nil {
		goutils.Logging(err)
	}
	defer ch.Close()

	q := getQueue("events", ch)

	setExchange("myExchange", "direct", "myRoutingKey", q.Name, ch)

	body := "abc"
	for i := 1; i <= 1000; i++ {
		err = ch.Publish(
			"myExchange",
			"myRoutingKey",
			false,
			false,
			amqp.Publishing{
				ContentType:  "text/plain",
				Body:         []byte(body + strconv.Itoa(i)),
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
			},
		)
		if err != nil {
			goutils.Logging(err)
		}
	}
	go func() {
		for i := 1001; i <= 2000; i++ {
			err = ch.Publish(
				"myExchange",
				"myRoutingKey",
				false,
				false,
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         []byte(body + strconv.Itoa(i)),
					DeliveryMode: amqp.Persistent,
					Timestamp:    time.Now(),
				},
			)
			if err != nil {
				goutils.Logging(err)
			}
		}
	}()
	go func() {
		for i := 2001; i <= 3000; i++ {
			err = ch.Publish(
				"myExchange",
				"myRoutingKey",
				false,
				false,
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         []byte(body + strconv.Itoa(i)),
					DeliveryMode: amqp.Persistent,
					Timestamp:    time.Now(),
				},
			)
			if err != nil {
				goutils.Logging(err)
			}
		}
	}()
	fmt.Println("publication complete")

	conn1 := getConnect("ruser", "rpassword", "localhost", "5673")

	ch1, err := conn.Channel()
	if err != nil {
		goutils.Logging(err)
	}
	defer ch1.Close()

	rabbit1 := learn.MyRabbitmq{}
	rabbit1.Conn = conn1
	rabbit1.Channel = ch1

	msgs, err := rabbit1.Consume(context.Background(), "events")
	if err != nil {
		goutils.Logging(err)
	}

	go func() {
		for d := range msgs {
			fmt.Println(string(d.Body))
			time.Sleep(20 * time.Microsecond)
			err := d.Ack(false)
			if err != nil {
				goutils.Logging(err)
			}
		}
	}()

	select {}
}

func getConnect(login, password, ip, port string) *amqp.Connection {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", login, password, ip, port))
	if err != nil {
		goutils.Logging(err)
	}
	return conn
}

func getQueue(queueTitle string, ch *amqp.Channel) amqp.Queue {
	q, err := ch.QueueDeclare(queueTitle, true, false, false, true, nil)
	if err != nil {
		goutils.Logging(err)
	}
	return q
}

func setExchange(exchangeTitle, exchangeType, routingKey, queueTitle string, ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		exchangeTitle,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		goutils.Logging(err)
	}
	err = ch.QueueBind(
		queueTitle,
		routingKey,
		exchangeTitle,
		false,
		nil,
	)
	if err != nil {
		goutils.Logging(err)
	}
}
