from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'ecommerce.orders',
    bootstrap_servers='localhost:9092',
    group_id='order-analytics',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer Instance 2 Started")

order_count = defaultdict(int)

for message in consumer:
    data = message.value

    order_count[data['user_id']] += 1

    print(f"\n[Consumer-2] Partition: {message.partition}, Offset: {message.offset}")
    print("Received:", data)

    print("Orders Per User:")
    for user, count in order_count.items():
        print(f"{user}: {count}")