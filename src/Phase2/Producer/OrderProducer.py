import csv
import json
from kafka import KafkaProducer

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# CSV File Path
csv_file = r"C:\Users\Lenovo\Desktop\kafka\Kafka-Ecommerce-Analytics\data\orders.csv"

# Read CSV and Publish Messages
with open(csv_file, 'r') as file:
    reader = csv.DictReader(file)

    for row in reader:

        # order_id is used as Kafka message key
        future = producer.send(
            'ecommerce.orders',
            key=row['order_id'],
            value=row
        )

        # Get metadata
        metadata = future.get(timeout=10)

        print(
            f"Order={row['order_id']} | "
            f"Partition={metadata.partition} | "
            f"Offset={metadata.offset}"
        )

producer.flush()
producer.close()

print("\nAll messages published successfully!")