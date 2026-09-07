package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func dispatchPoliceAlert(sensorEvent SensorData, msg string, severity string, policeAddress string) {
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
						log.Printf("[!!!] CRITICAL BREACH: Unauthorized movement detected in B4-TYRANT-LAB! Sensor: %s", data.SensorID)
					case "Chemical Experiment Room":
						log.Printf("[!] SECURITY: Movement in restricted Chemical Area! Sensor: %s", data.SensorID)
					default:
						log.Printf("[INFO] Routine movement tracked in %s (Sensor: %s)", data.Room, data.SensorID)
					}
				}

			case "TEMP":
				switch data.Room {
				case "B4-TYRANT-LAB":
					if data.Value > 18.5 {
						// Baseline 15.0 -> >18.0 means cryogenic cooling is failing
						log.Printf("[!!!] ALARM: B4-TYRANT-LAB temperature critical! Specimen thawing risk (%.2f°C)", data.Value)
					}
				case "Power Room":
					if data.Value > 48.0 {
						// Baseline 45.0 -> Overheating
						log.Printf("[WARNING] Power Room overheating! Generator stress detected (%.2f°C)", data.Value)
					}
				case "Chemical Experiment Room":
					if data.Value > 21.0 || data.Value < 15.0 {
						// Baseline 18.5 -> Chemical volatility
						log.Printf("[WARNING] Chemical Experiment Room temperature unstable! (%.2f°C)", data.Value)
					}
				case "Visual Data Room":
					if data.Value > 25.0 {
						// Baseline 22.0 -> Server overheating
						log.Printf("[WARNING] Visual Data Room servers at thermal risk! (%.2f°C)", data.Value)
					}
				}

			case "MOIST":
				switch data.Room {
				case "Power Room":
					if data.Value > 33.5 {
						// Baseline 30.0 -> Danger of electrical short
						log.Printf("[!!!] ALARM: High humidity in Power Room! Short circuit risk (%.2f%%)", data.Value)
					}
				case "Waste Disposal Plant":
					if data.Value > 88.0 {
						// Baseline 85.0 -> Overflow/Leakage
						log.Printf("[WARNING] Waste Disposal Plant moisture overflow risk (%.2f%%)", data.Value)
					}
				case "Visual Data Room":
					if data.Value > 43.0 {
						// Baseline 40.0 -> Hardware corrosion risk
						log.Printf("[WARNING] Elevated humidity near server racks in Visual Data Room (%.2f%%)", data.Value)
					}
				}

			case "PRESSURE":
				if data.Room == "Chemical Experiment Room" {
					if data.Value > 1016.5 {
						// Baseline 1013.0 -> Explosion risk
						log.Printf("[!!!] ALARM: High pressure in Chemical Experiment Room! Venting required (%.2f hPa)", data.Value)
					} else if data.Value < 1009.5 {
						// Baseline 1013.0 -> Containment leak
						log.Printf("[!!!] ALARM: Pressure drop in Chemical Experiment Room! Containment leak? (%.2f hPa)", data.Value)
					}
				}
				d.Ack(false)
			}
		}
	}()

	log.Printf("[*] Waiting for messages. To end, press Ctrl+C.")
	<-forever
}
