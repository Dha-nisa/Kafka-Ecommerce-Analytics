# earnings_driver.py

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ride.completed',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

earnings = {}

print("Driver Earnings Consumer Started")

for message in consumer:

    ride = message.value

    driver = ride["driver_id"]

    if driver not in earnings:
        earnings[driver] = {
            "rides": 0,
            "earnings": 0
        }

    earnings[driver]["rides"] += 1
    earnings[driver]["earnings"] += 5

    print("\nDriver:", driver)
    print("Completed Rides:", earnings[driver]["rides"])
    print("Total Earnings: $", earnings[driver]["earnings"])