package learn

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
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
		invalidDataMsg := "Check data"
		err := json.Unmarshal(msg.Body, &data)
		if err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		envData := goutils.GetConfFromEnvFile("")
		envKeys := goutils.GetMapKeysWithValue(envData)
		neededKeys := []string{
			"SMTP_HOST",
			"SMTP_PORT",
			"SMTP_USERNAME",
			"SMTP_PASSWORD",
			"FROM",
		}
		for _, val := range neededKeys {
			if !slices.Contains(envKeys, val) {
				fmt.Println("no smtp credentials")
				return
			}
		}
		smtpHost := goutils.ConcatSlice([]string{envData["SMTP_HOST"], ":", envData["SMTP_PORT"]})

		auth := smtp.PlainAuth("", envData["SMTP_USERNAME"], envData["SMTP_PASSWORD"], envData["SMTP_HOST"])
		from := mail.Address{Address: envData["FROM"]}
		to := mail.Address{Address: data["email"]}
		host, _, _ := net.SplitHostPort(smtpHost)
		tlsconfig := &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         host,
		}

		conn, err := tls.Dial("tcp", smtpHost, tlsconfig)
		if err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		c, err := smtp.NewClient(conn, host)
		if err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		if err = c.Auth(auth); err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		if err = c.Mail(from.Address); err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		if err = c.Rcpt(to.Address); err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		w, err := c.Data()
		if err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		message := []byte(goutils.ConcatSlice([]string{"To: ",
			data["email"],
			"\r\n",
			"Subject: Hello from Go!\r\n",
			"\r\n",
			"This is a test email sent from Golang.",
		}))
		_, err = w.Write([]byte(message))
		if err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		err = w.Close()
		if err != nil {
			goutils.Logging(err)
			fmt.Println(invalidDataMsg)
			return
		}
		c.Quit()
		fmt.Println("Email sent successfully!")
	}
}
