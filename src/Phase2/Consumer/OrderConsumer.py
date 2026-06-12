import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'ecommerce.orders',
    bootstrap_servers='localhost:9092',
    group_id='order-analytics',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Consumer started...")

for message in consumer:
    try:
        data = json.loads(message.value.decode('utf-8'))

        print("\nValid Message Received")
        print(data)

    except json.JSONDecodeError:
        print("\nPOISON MESSAGE DETECTED")
        print(f"Raw Value: {message.value.decode('utf-8')}")
        print("Skipping message...")
        print("-" * 50)