package main

type PoliceAlert struct {
	SensorData SensorData `json:"sensor_data"`
	Message    string     `json:"message"`
	Severity   Severity   `json:"severity"`
}
