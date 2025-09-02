package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"rabbitmqlearn/learn"
	"sync"

	goutils "github.com/Corax73/goUtils"
	"github.com/gorilla/mux"
)

func main() {
	goutils.LogInit("")

	config1 := learn.ConnectConfig{
		Login:         "ruser",
		Password:      "rpassword",
		Ip:            "localhost",
		Port:          "5673",
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
		Login:         "ruser",
		Password:      "rpassword",
		Ip:            "localhost",
		Port:          "5673",
		QueueTitle:    "events2",
		ExchangeTitle: "myExchange",
		ExchangeType:  "direct",
		RoutingKey:    "myRoutingKey2",
		ConsumerTitle: "consumer2",
		Durable:       true,
		NoWait:        true,
	}
	rabbit2 := learn.Init(&config2)

	r := mux.NewRouter()
	r.HandleFunc("/publish1/", func(w http.ResponseWriter, r *http.Request) {
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
	r.HandleFunc("/publish2/", func(w http.ResponseWriter, r *http.Request) {
		postParams := make(map[string]any)
		err := json.NewDecoder(r.Body).Decode(&postParams)
		if err != nil {
			goutils.Logging(err)
		}
		fmt.Println(postParams)
		if val, ok := postParams["message"]; ok {
			result := rabbit2.Publish(val.(string))
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
	worker1.Run()
	worker2.Run()

	select {}
}
