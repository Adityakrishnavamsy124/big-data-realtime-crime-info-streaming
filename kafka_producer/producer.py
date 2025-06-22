import csv
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka Config
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crime_stream")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Path to CSV file
CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'crimes_sampled.csv')

# Produce messages
with open(CSV_PATH, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Send the row as a JSON event
        producer.send(KAFKA_TOPIC, value=row)
        print(f"Sent: {row['ID'] if 'ID' in row else 'row'} to topic '{KAFKA_TOPIC}'")

        # Simulate real-time stream delay
        time.sleep(0.5)

producer.flush()
print("All rows sent.")
