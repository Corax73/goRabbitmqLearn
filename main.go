package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"rabbitmqlearn/learn"
	"sync"
	"time"

	goutils "github.com/Corax73/goUtils"
	"github.com/gorilla/mux"
)

func main() {
	goutils.LogInit("")

	config := learn.ConnectConfig{
		Login:         "ruser",
		Password:      "rpassword",
		Ip:            "localhost",
		Port:          "5673",
		QueueTitle:    "events",
		ExchangeTitle: "myExchange",
		ExchangeType:  "direct",
		RoutingKey:    "myRoutingKey",
		ConsumerTitle: "consumer1",
		Durable:       true,
		AutoDelete:    false,
		Exclusive:     false,
		NoWait:        true,
		Internal:      false,
		Mandatory:     false,
		Immediate:     false,
		AutoAck:       false,
		NoLocal:       false,
		Args:          nil,
	}
	rabbit1 := learn.Init(&config)

	r := mux.NewRouter()
	r.HandleFunc("/publish/", func(w http.ResponseWriter, r *http.Request) {
		postParams := make(map[string]any)
		err := json.NewDecoder(r.Body).Decode(&postParams)
		if err != nil {
			goutils.Logging(err)
		}
		fmt.Println(postParams)
		if val, ok := postParams["message"]; ok {
			result := rabbit1.Publish(val.(string))
			if result {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]int{"success": 1})
			} else {
				w.WriteHeader(http.StatusFailedDependency)
				json.NewEncoder(w).Encode(map[string]int{"success": 0})
			}
		}
	}).Methods("POST")
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func(handler http.Handler) {
		http.ListenAndServe(":8083", handler)
		defer wg.Done()
	}(r)

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
