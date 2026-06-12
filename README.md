# Kafka-Ecommerce-Analytics
# Apache Kafka Event Streaming System

This repository demonstrates an end-to-end event streaming architecture built using Apache Kafka and Python. The project covers Kafka fundamentals, producer-consumer communication, consumer groups, partition rebalancing, poison message handling, and a ride-sharing event processing pipeline.

---

## Technologies Used

- Apache Kafka
- ZooKeeper
- Python
- kafka-python
- Docker Compose
- GitHub

---

# Kafka Mode Used

This project uses **ZooKeeper Mode**.

ZooKeeper is responsible for cluster coordination, broker management, leader election, and metadata storage. Kafka brokers communicate with ZooKeeper to maintain cluster state. Although newer Kafka versions support KRaft mode, this project was implemented using ZooKeeper for simplicity and compatibility with local development environments.

---

# Project Structure

```text
Kafka-Ecommerce-Analytics/
│
├── data/
│   ├── orders.csv
│   └── Screenshots/
│
├── docs/
│   ├── Phase1/
│   └── Phase2/
│
├── src/
│   ├── Phase2/
│   │   ├── Producer/
│   │   │   ├── OrderProducer.py
│   │   │   └── PoisonProducer.py
│   │   │
│   │   └── Consumer/
│   │       ├── OrderConsumer.py
│   │       └── OrderConsumerInstance2.py
│   │
│   └── Phase3/
│       ├── ride_producer.py
│       ├── completed_consumer.py
│       ├── earnings_driver.py
│       └── top_drivers.py
│
├── docker-compose.yml
├── README.md
└── pom.xml
```

---

# Setup Instructions
## Phase 1
## Step 1 - Install Dependencies

```bash
pip install kafka-python
```

## Step 2 - Start Kafka and ZooKeeper

```bash
docker compose up -d
```
![Kafka Started](data/Screenshots/1.Kafka_Started.png)


Verify containers:

```bash
docker ps
```
![Broker Status Check](data/Screenshots/2.2Broker%20Status%20Check.png)

---

## Step 3 - Create Topics

### Phase 2 Topic

```bash
docker exec -it kafka kafka-topics \
--create \
--topic ecommerce.orders \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```
![Topic Created](data/Screenshots/2.1Topic_Created.png)

---



# Phase 2 - E-Commerce Orders Pipeline

## Topic Used

```text
ecommerce.orders
```

![Phase2 Topic Created](data/Screenshots/5.1Phase2TopicCreated.png)

## Dataset

The producer reads order events from:

```text
data/orders.csv
```

Fields:

- order_id
- user_id
- product_id
- amount
- timestamp

---

## Task 2.1 - Producer

Reads records from CSV and publishes them to Kafka using `order_id` as the message key.

Run:

```bash
python src/Phase2/Producer/OrderProducer.py
```
![Message Produced](data/Screenshots/3.Message_Produced.png)

---

## Task 2.2 - Consumer

Consumes messages from `ecommerce.orders`, prints records, and maintains a running count of orders per user.

Run:

```bash
python src/Phase2/Consumer/OrderConsumer.py
```
![Message Consumed](data/Screenshots/4.Message_Consumed.png)


```bash
Adding Message
```
![Adding Message](data/Screenshots/5.2Adding_Message.png)

![Output Consume](data/Screenshots/5.3Output_Consume.png)

---


## Task 2.3 - Consumer Rebalancing

Run two consumers in the same group.

Terminal 1:

```bash
python src/Phase2/Consumer/OrderConsumer.py
```
![Output](data/Screenshots/6.3.1Phase2%20Consumer1.png)

Terminal 2:

```bash
python src/Phase2/Consumer/OrderConsumerInstance2.py
```
![Output](data/Screenshots/6.3.2Phase2%20Consumer2.png)

Demonstrates partition assignment and consumer group rebalancing.

---

## Task 2.4 - Poison Message Handling

### Poison Producer

```bash
python src/Phase2/Producer/PoisonProducer.py
```
![Poison Producer](data/Screenshots/6.4.1ProducerPoison.png)
### Poison Consumer Output

```text
POISON MESSAGE DETECTED

Raw Value: INVALID_JSON_MESSAGE

Skipping message...
```
![Poison Message](data/Screenshots/6.4.2PoisonMessage.png)

---

# Phase 3 - Ride Sharing Pipeline

## Topics Used

### ride.events

Stores all ride events.

Fields:

- ride_id
- driver_id
- rider_id
- status
- lat
- lon
- timestamp

Statuses:

- REQUESTED
- ACCEPTED
- COMPLETED
- CANCELLED

### ride.completed

Stores only completed ride events.

### driver.earnings

Stores aggregated driver statistics.

---

## Pipeline Flow

```text
Ride Producer
      │
      ▼
 ride.events
      │
      ▼
Completed Ride Consumer
      │
      ▼
 ride.completed
      │
      ▼
Driver Earnings Consumer
      │
      ▼
Top Drivers CLI
```

---

## Task 3.1 - Ride Producer

```bash
python src/Phase3/ride_producer.py
```
![Output](data/Screenshots/7.1Ride_Producer.png)
---

## Task 3.2 - Completed Ride Consumer

```bash
python src/Phase3/completed_consumer.py
```
![Output](data/Screenshots/7.2Completed_Consumer.png)

Filters only completed rides and forwards them to `ride.completed`.

---

## Task 3.3 - Driver Earnings Aggregator

```bash
python src/Phase3/earnings_driver.py
```

Flat rate:

```text
$5 per completed ride
```
![Output](data/Screenshots/7.3Driver_Earnings.png)
---

## Task 3.4 - Top Drivers CLI

```bash
python src/Phase3/top_drivers.py
```

Displays top drivers ranked by completed rides.

![Output](data/Screenshots/7.5Top_Drivers.png)
---

# Screenshots

Evidence and execution outputs are available under:

```text
data/Screenshots/
```

- Phase1 screenshots
- Producer outputs
- Consumer outputs
- Rebalancing demo
- Poison message handlinga
- Ride-sharing pipeline execution

---

Conclusion

This repository demonstrates Apache Kafka fundamentals and event-driven application development using Python and ZooKeeper through:

E-Commerce Orders Processing System
Ride Sharing Event Analytics Pipeline

The implementation covers producer-consumer communication, consumer groups, partition rebalancing, poison message handling, event filtering, and real-time analytics using Apache Kafka.