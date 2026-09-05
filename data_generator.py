import json
import random
import pika
import time
from datetime import datetime, timezone

def data_generator():
    print("Initializing the sensors...")
    active_sensors = initializeSensors("rooms.json")

    HOST = "localhost"
    PORT = 5672

    credentials = pika.PlainCredentials("admin", "secret")

    parameters = pika.ConnectionParameters(
        host=HOST,
        port=PORT,
        virtual_host="/",
        credentials=credentials
        )

    print("Building the connection...")
    connection = pika.BlockingConnection(parameters)

    print("Opening the connection channel...")
    channel = connection.channel()

    print("Declaring the exchange...")
    channel.exchange_declare(exchange='umbrella_sensors', exchange_type='topic')
    try:
        while True:
            random.shuffle(active_sensors)
            for sensor in active_sensors:
                sensor_id = sensor["sensor_id"]
                room = sensor["room"]
                sensor_type = sensor["type"]
                baseline = sensor["baseline"]
                timestamp = datetime.now(timezone.utc).isoformat()


                if sensor_type in ["TEMP", "MOIST", "PRESSURE"]:
                    value = round(random.gauss(baseline, 1.0), 2)
                else:
                    if random.random() < 0.05:
                        value = 1
                    else:
                        value = 0

                data_point = {
                    "timestamp": timestamp,
                    "sensor_id": sensor_id,
                    "room": room,
                    "type": sensor_type,
                    "value": value
                }

                routing_key = f"sensors.{sensor_type.lower()}.{room.replace(' ', '_').lower()}"
                channel.basic_publish(exchange='umbrella_sensors', routing_key=routing_key, body=json.dumps(data_point))
                print(f"[x] Sent: {routing_key} -> {value}")

                time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
            print("\n[!] Shutting down the labaratory. Closing the connection...")
            


def initializeSensors(filepath):
    with open(filepath, mode='r') as f:
        rooms_config = json.load(f)

        active_sensors = []

        for room in rooms_config:
            room_name = room['room_name']

            for key, value in room.items():
                if key.endswith('_num'):
                    sensor_prefix = key.split('_')[0]
                    sensor_type = sensor_prefix.upper()
                    sensor_count = value

                    baseline_key = f"{sensor_prefix}_baseline"
                    baseline_value = room.get(baseline_key, None)

                    for i in range(1, sensor_count + 1):
                        safe_room_name = room_name.replace(' ', '_')
                        sensor_id = f"{sensor_type}-{safe_room_name}-{i}"

                        sensor = {
                            "sensor_id": sensor_id,
                            "room": room_name,
                            "type": sensor_type,
                            "baseline": baseline_value
                        }

                        active_sensors.append(sensor)

    return active_sensors
    

if __name__ == "__main__":
    data_generator()