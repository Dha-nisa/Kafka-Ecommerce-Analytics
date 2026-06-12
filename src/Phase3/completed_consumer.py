# completed_consumer.py

from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'ride.events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Completed Ride Consumer Started")

for message in consumer:

    ride = message.value

    print("Received:", ride)

    if ride["status"] == "COMPLETED":

        producer.send(
            "ride.completed",
            value=ride
        )

        print("Forwarded Completed Ride:", ride["ride_id"])