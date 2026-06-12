# ride_producer.py

from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

rides = [
    {
        "ride_id": "R001",
        "driver_id": "D001",
        "rider_id": "U001",
        "status": "REQUESTED",
        "lat": 9.9312,
        "lon": 76.2673,
        "timestamp": "2026-06-11T15:00:00"
    },
    {
        "ride_id": "R002",
        "driver_id": "D002",
        "rider_id": "U002",
        "status": "COMPLETED",
        "lat": 9.9340,
        "lon": 76.2700,
        "timestamp": "2026-06-11T15:01:00"
    },
    {
        "ride_id": "R003",
        "driver_id": "D001",
        "rider_id": "U003",
        "status": "COMPLETED",
        "lat": 9.9350,
        "lon": 76.2710,
        "timestamp": "2026-06-11T15:02:00"
    },
    {
        "ride_id": "R004",
        "driver_id": "D003",
        "rider_id": "U004",
        "status": "CANCELLED",
        "lat": 9.9360,
        "lon": 76.2720,
        "timestamp": "2026-06-11T15:03:00"
    },
    {
        "ride_id": "R005",
        "driver_id": "D002",
        "rider_id": "U005",
        "status": "COMPLETED",
        "lat": 9.9370,
        "lon": 76.2730,
        "timestamp": "2026-06-11T15:04:00"
    },

    {
        "ride_id": "R006",
        "driver_id": "D004",
        "rider_id": "U006",
        "status": "ACCEPTED",
        "lat": 9.9380,
        "lon": 76.2740,
        "timestamp": "2026-06-11T15:05:00"
    },
    {
        "ride_id": "R007",
        "driver_id": "D001",
        "rider_id": "U007",
        "status": "COMPLETED",
        "lat": 9.9390,
        "lon": 76.2750,
        "timestamp": "2026-06-11T15:06:00"
    },
    {
        "ride_id": "R008",
        "driver_id": "D005",
        "rider_id": "U008",
        "status": "REQUESTED",
        "lat": 9.9400,
        "lon": 76.2760,
        "timestamp": "2026-06-11T15:07:00"
    },
    {
        "ride_id": "R009",
        "driver_id": "D002",
        "rider_id": "U009",
        "status": "COMPLETED",
        "lat": 9.9410,
        "lon": 76.2770,
        "timestamp": "2026-06-11T15:08:00"
    },
    {
        "ride_id": "R010",
        "driver_id": "D003",
        "rider_id": "U010",
        "status": "CANCELLED",
        "lat": 9.9420,
        "lon": 76.2780,
        "timestamp": "2026-06-11T15:09:00"
    },
    {
        "ride_id": "R011",
        "driver_id": "D004",
        "rider_id": "U011",
        "status": "COMPLETED",
        "lat": 9.9430,
        "lon": 76.2790,
        "timestamp": "2026-06-11T15:10:00"
    },
    {
        "ride_id": "R012",
        "driver_id": "D005",
        "rider_id": "U012",
        "status": "ACCEPTED",
        "lat": 9.9440,
        "lon": 76.2800,
        "timestamp": "2026-06-11T15:11:00"
    },
    {
        "ride_id": "R013",
        "driver_id": "D001",
        "rider_id": "U013",
        "status": "COMPLETED",
        "lat": 9.9450,
        "lon": 76.2810,
        "timestamp": "2026-06-11T15:12:00"
    },
    {
        "ride_id": "R014",
        "driver_id": "D002",
        "rider_id": "U014",
        "status": "REQUESTED",
        "lat": 9.9460,
        "lon": 76.2820,
        "timestamp": "2026-06-11T15:13:00"
    },
    {
        "ride_id": "R015",
        "driver_id": "D003",
        "rider_id": "U015",
        "status": "COMPLETED",
        "lat": 9.9470,
        "lon": 76.2830,
        "timestamp": "2026-06-11T15:14:00"
    }
]

for ride in rides:
    producer.send("ride.events", value=ride)
    print("Sent:", ride)
    time.sleep(1)

producer.flush()
print("All ride events published successfully!")