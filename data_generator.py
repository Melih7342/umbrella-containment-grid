import os
import json

def data_generator():
    data = {}
    while True:
        for i in range()


def generate_data(dict, filepath):
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
                    baseline_value = room[baseline_key]

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
