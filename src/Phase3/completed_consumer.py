from kafka import KafkaConsumer, KafkaProducer
import json

# Consumer: Reads from ride.events
consumer = KafkaConsumer(
    'ride.events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # Read only new messages
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Producer: Writes only completed rides to ride.completed
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Completed Ride Consumer Started...")

for message in consumer:

    ride = message.value

    # Filter only COMPLETED rides
    if ride["status"] == "COMPLETED":

        producer.send(
            "ride.completed",
            value=ride
        )

        producer.flush()

        print(f"Forwarded Completed Ride: {ride['ride_id']}")