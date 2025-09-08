package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"rabbitmqlearn/learn"
	"slices"
	"sync"

	goutils "github.com/Corax73/goUtils"
	"github.com/gorilla/mux"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	goutils.LogInit("")

	envData := goutils.GetConfFromEnvFile("")
	envKeys := goutils.GetMapKeysWithValue(envData)
	neededKeys := []string{
		"RMQ_HOST",
		"RMQ_PORT",
		"RMQ_LOGIN",
		"RMQ_PASSWORD",
	}
	for _, val := range neededKeys {
		if !slices.Contains(envKeys, val) {
			fmt.Println("no rmq credentials")
			return
		}
	}
	config1 := learn.ConnectConfig{
		Login:         envData["RMQ_LOGIN"],
		Password:      envData["RMQ_PASSWORD"],
		Ip:            envData["RMQ_HOST"],
		Port:          envData["RMQ_PORT"],
		QueueTitle:    "events1",
		ExchangeTitle: "myExchange",
		ExchangeType:  "direct",
		RoutingKey:    "myRoutingKey1",
		ConsumerTitle: "consumer1",
		Durable:       true,
		NoWait:        true,
	}
	rabbit1 := learn.Init(&config1)

	config2 := learn.ConnectConfig{
		Login:         envData["RMQ_LOGIN"],
		Password:      envData["RMQ_PASSWORD"],
		Ip:            envData["RMQ_HOST"],
		Port:          envData["RMQ_PORT"],
		QueueTitle:    "events2",
		ExchangeTitle: "myExchange",
		ExchangeType:  "direct",
		RoutingKey:    "myRoutingKey2",
		ConsumerTitle: "consumer2",
		Durable:       true,
		NoWait:        true,
	}
	rabbit2 := learn.Init(&config2)

	config3 := learn.ConnectConfig{
		Login:         envData["RMQ_LOGIN"],
		Password:      envData["RMQ_PASSWORD"],
		Ip:            envData["RMQ_HOST"],
		Port:          envData["RMQ_PORT"],
		QueueTitle:    "events3",
		ExchangeTitle: "myExchange",
		ExchangeType:  "direct",
		RoutingKey:    "myRoutingKey3",
		ConsumerTitle: "consumer3",
		Durable:       true,
		NoWait:        true,
	}
	rabbit3 := learn.Init(&config3)

	r := mux.NewRouter()
	r.HandleFunc("/publish1/", func(w http.ResponseWriter, r *http.Request) {
		postParams := make(map[string]any)
		err := json.NewDecoder(r.Body).Decode(&postParams)
		if err != nil {
			goutils.Logging(err)
		}
		if val, ok := postParams["message"]; ok {
			jsonBytes, err := json.Marshal(val)
			if err != nil {
				goutils.Logging(err)
			}
			result := rabbit1.Publish(jsonBytes)
			if result {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]int{"success": 1})
			} else {
				w.WriteHeader(http.StatusFailedDependency)
				json.NewEncoder(w).Encode(map[string]int{"success": 0})
			}
		}
	}).Methods("POST")
	r.HandleFunc("/publish2/", func(w http.ResponseWriter, r *http.Request) {
		postParams := make(map[string]any)
		err := json.NewDecoder(r.Body).Decode(&postParams)
		if err != nil {
			goutils.Logging(err)
		}
		if val, ok := postParams["message"]; ok {
			jsonBytes, err := json.Marshal(val)
			if err != nil {
				goutils.Logging(err)
			}
			result := rabbit2.Publish(jsonBytes)
			if result {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]int{"success": 1})
			} else {
				w.WriteHeader(http.StatusFailedDependency)
				json.NewEncoder(w).Encode(map[string]int{"success": 0})
			}
		}
	}).Methods("POST")
	r.HandleFunc("/send/", func(w http.ResponseWriter, r *http.Request) {
		postParams := make(map[string]any)
		err := json.NewDecoder(r.Body).Decode(&postParams)
		if err != nil {
			goutils.Logging(err)
		}
		if val, ok := postParams["message"]; ok {
			jsonBytes, err := json.Marshal(val)
			if err != nil {
				goutils.Logging(err)
			}
			result := rabbit3.Publish(jsonBytes)
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
	wg.Go(func() {
		http.ListenAndServe(":8083", r)
	})

	worker1 := learn.Worker{
		QueueTitle: config1.QueueTitle,
		Rmq:        rabbit1,
	}

	worker2 := learn.Worker{
		QueueTitle: config2.QueueTitle,
		Rmq:        rabbit2,
	}
	worker3 := learn.Worker{
		QueueTitle: config3.QueueTitle,
		Rmq:        rabbit3,
	}
	senderWorker := learn.GetSender(&worker3)
	callback := func(worker *learn.Worker, msg amqp091.Delivery) {
		fmt.Println(worker.Rmq.GetConfig().ConsumerTitle, string(msg.Body))
	}
	worker1.Run(callback)
	worker2.Run(callback)
	senderWorker.Run(senderWorker.GetHandle())

	select {}
}
