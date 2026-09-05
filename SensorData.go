package main

type SensorData struct {
	Timestamp string  `json:"timestamp"`
	SensorID  string  `json:"sensor_id"`
	Room      string  `json:"room"`
	Type      string  `json:"type"`
	Value     float64 `json:"value"`
}
