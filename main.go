package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const policeAddress = "http://localhost:8083/alert"

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func dispatchPoliceAlert(sensorEvent SensorData, msg string, severity Severity, policeAddress string) {
	policeAlert := PoliceAlert{
		SensorData: sensorEvent,
		Message:    msg,
		Severity:   severity,
	}

	jsonRequest, err := json.Marshal(policeAlert)
	if err != nil {
		log.Printf("Could not marshal the Police Alert: %v", err)
		return
	}

	bodyReader := bytes.NewBuffer(jsonRequest)

	client := &http.Client{Timeout: 10 * time.Second}

	response, err := client.Post(policeAddress, "application/json", bodyReader)
	if err != nil {
		log.Printf("Could not send the alarm to the Police Station: %v", err)
		return
	}

	defer response.Body.Close()

	log.Printf("Alarm successfully sent, Police Station responds: %s", response.Status)
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
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Error registering the consumer")

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			log.Printf("Reveived message: %s", string(d.Body))

			var data SensorData

			err := json.Unmarshal(d.Body, &data)
			if err != nil {
				log.Printf("Error parsing the data %s", err)

				d.Reject(false)
				continue
			}

			switch data.Type {
			case "MOTION":
				if data.Value == 1.0 {
					switch data.Room {
					case "B4-TYRANT-LAB":
						msg := fmt.Sprintf("Unauthorized movement detected in B4-TYRANT-LAB! Sensor: %s", data.SensorID)
						log.Printf("[!!!] CRITICAL BREACH: %s", msg)
						dispatchPoliceAlert(data, msg, SeverityCritical, policeAddress)

					case "Chemical Experiment Room":
						msg := fmt.Sprintf("Movement in restricted Chemical Area! Sensor: %s", data.SensorID)
						log.Printf("[!] SECURITY: %s", msg)
						dispatchPoliceAlert(data, msg, SeverityWarning, policeAddress)

					default:
						log.Printf("[INFO] Routine movement tracked in %s (Sensor: %s)", data.Room, data.SensorID)
					}
				}

			case "TEMP":
				switch data.Room {
				case "B4-TYRANT-LAB":
					if data.Value > 18.5 {
						msg := fmt.Sprintf("B4-TYRANT-LAB temperature critical! Specimen thawing risk (%.2f°C)", data.Value)
						log.Printf("[!!!] ALARM: %s", msg)
						dispatchPoliceAlert(data, msg, SeverityCritical, policeAddress)
					}
				case "Power Room":
					if data.Value > 48.0 {
						msg := fmt.Sprintf("Power Room overheating! Generator stress detected (%.2f°C)", data.Value)
						log.Printf("[WARNING] %s", msg)
						dispatchPoliceAlert(data, msg, SeverityWarning, policeAddress)
					}
				case "Chemical Experiment Room":
					if data.Value > 21.0 || data.Value < 15.0 {
						msg := fmt.Sprintf("Chemical Experiment Room temperature unstable! (%.2f°C)", data.Value)
						log.Printf("[WARNING] %s", msg)
						dispatchPoliceAlert(data, msg, SeverityWarning, policeAddress)
					}
				case "Visual Data Room":
					if data.Value > 25.0 {
						msg := fmt.Sprintf("Visual Data Room servers at thermal risk! (%.2f°C)", data.Value)
						log.Printf("[WARNING] %s", msg)
						dispatchPoliceAlert(data, msg, SeverityWarning, policeAddress)
					}
				}

			case "MOIST":
				switch data.Room {
				case "Power Room":
					if data.Value > 33.5 {
						msg := fmt.Sprintf("High humidity in Power Room! Short circuit risk (%.2f%%)", data.Value)
						log.Printf("[!!!] ALARM: %s", msg)
						dispatchPoliceAlert(data, msg, SeverityCritical, policeAddress)
					}
				case "Waste Disposal Plant":
					if data.Value > 88.0 {
						msg := fmt.Sprintf("Waste Disposal Plant moisture overflow risk (%.2f%%)", data.Value)
						log.Printf("[WARNING] %s", msg)
						dispatchPoliceAlert(data, msg, SeverityWarning, policeAddress)
					}
				case "Visual Data Room":
					if data.Value > 43.0 {
						msg := fmt.Sprintf("Elevated humidity near server racks in Visual Data Room (%.2f%%)", data.Value)
						log.Printf("[WARNING] %s", msg)
						dispatchPoliceAlert(data, msg, SeverityWarning, policeAddress)
					}
				}

			case "PRESSURE":
				if data.Room == "Chemical Experiment Room" {
					if data.Value > 1016.5 {
						msg := fmt.Sprintf("High pressure in Chemical Experiment Room! Venting required (%.2f hPa)", data.Value)
						log.Printf("[!!!] ALARM: %s", msg)
						dispatchPoliceAlert(data, msg, SeverityCritical, policeAddress)

					} else if data.Value < 1009.5 {
						msg := fmt.Sprintf("Pressure drop in Chemical Experiment Room! Containment leak? (%.2f hPa)", data.Value)
						log.Printf("[!!!] ALARM: %s", msg)
						dispatchPoliceAlert(data, msg, SeverityCritical, policeAddress)
					}
				}
				d.Ack(false)
			}
		}
	}()

	log.Printf("[*] Waiting for messages. To end, press Ctrl+C.")
	<-forever
}
