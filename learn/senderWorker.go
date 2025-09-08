package learn

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/mail"
	"net/smtp"
	"slices"

	goutils "github.com/Corax73/goUtils"
	"github.com/rabbitmq/amqp091-go"
)

type SenderWorker struct {
	*Worker
}

type OperationCallback func(*Worker, amqp091.Delivery)

func GetSender(worker *Worker) *SenderWorker {
	return &SenderWorker{worker}
}

func (senderWorker *SenderWorker) GetHandle() OperationCallback {
	return func(worker *Worker, msg amqp091.Delivery) {
		var data map[string]string
		err := json.Unmarshal(msg.Body, &data)
		if err != nil {
			fmt.Println("Error unmarshaling JSON:", err)
		}
		fmt.Println(data)
		envData := goutils.GetConfFromEnvFile("")
		envKeys := goutils.GetMapKeysWithValue(envData)
		neededKeys := []string{
			"SMTP_HOST",
			"SMTP_PORT",
			"SMTP_USERNAME",
			"SMTP_PASSWORD",
			"FROM",
		}
		check := true
		for _, val := range envKeys {
			if !slices.Contains(neededKeys, val) {
				check = false
				break
			}
		}
		if !check {
			fmt.Println("no credentials")
		} else {
			smtpHost := goutils.ConcatSlice([]string{envData["SMTP_HOST"], ":", envData["SMTP_PORT"]})

			auth := smtp.PlainAuth("", envData["SMTP_USERNAME"], envData["SMTP_PASSWORD"], envData["SMTP_HOST"])
			from := mail.Address{"", envData["FROM"]}
			to := mail.Address{"", data["email"]}
			host, _, _ := net.SplitHostPort(smtpHost)
			tlsconfig := &tls.Config{
				InsecureSkipVerify: true,
				ServerName:         host,
			}

			conn, err := tls.Dial("tcp", smtpHost, tlsconfig)
			if err != nil {
				log.Panic(err)
			}

			c, err := smtp.NewClient(conn, host)
			if err != nil {
				log.Panic(err)
			}

			// Auth
			if err = c.Auth(auth); err != nil {
				log.Panic(err)
			}

			// To && From
			if err = c.Mail(from.Address); err != nil {
				log.Panic(err)
			}

			if err = c.Rcpt(to.Address); err != nil {
				log.Panic(err)
			}

			// Data
			w, err := c.Data()
			if err != nil {
				log.Panic(err)
			}

			message := []byte("To: " + data["email"] + "\r\n" +
				"Subject: Hello from Go!\r\n" +
				"\r\n" +
				"This is a test email sent from Golang.")
			_, err = w.Write([]byte(message))
			if err != nil {
				log.Panic(err)
			}

			err = w.Close()
			if err != nil {
				log.Panic(err)
			}

			c.Quit()
			fmt.Println("Email sent successfully!")
		}
	}
}
