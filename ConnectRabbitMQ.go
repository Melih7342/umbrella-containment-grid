package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://admin:secret@localhost:5672/")
	failOnError(err, "Error connecting with RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Error opening the channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"lab_data_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Error declaring the queue")

	err = ch.QueueBind(
		q.Name,
		"sensors.#",
		"umbrella_sensors",
		false,
		nil,
	)
	failOnError(err, "Error binding the queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Error registering the consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Reveived message: %s", string(d.Body))
		}
	}()

	log.Printf("[*] Waiting for messages. To end, press Ctrl+C.")
	<-forever
}
