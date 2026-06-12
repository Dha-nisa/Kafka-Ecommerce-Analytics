# top_drivers.py

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ride.completed',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

driver_count = {}

print("Top Drivers Service Started")

for message in consumer:

    ride = message.value

    driver = ride["driver_id"]

    driver_count[driver] = driver_count.get(driver, 0) + 1

    sorted_drivers = sorted(
        driver_count.items(),
        key=lambda x: x[1],
        reverse=True
    )

    print("\n===== TOP DRIVERS =====")

    for index, (driver, rides) in enumerate(sorted_drivers[:5], start=1):
        print(f"{index}. {driver} -> {rides} rides")