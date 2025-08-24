from kafka import KafkaProducer
import json
import random
import time
from pymongo import MongoClient

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Set up MongoDB client and collection
client = MongoClient("mongodb://localhost:27017/")
db = client['anomaly_detection']
collection = db['anomaly_results']

def generate_sensor_data():
    return {
        'timestamp': int(time.time()),
        'temperature': round(random.uniform(20, 100), 2),
        'pressure': round(random.uniform(1, 10), 2),
        'vibration': round(random.uniform(0, 1), 3)
    }

while True:
    try:
        data = generate_sensor_data()
        # Send to Kafka topic
        producer.send('iot-sensor-data', data)
        producer.flush()   # Ensure data is sent immediately

        # Insert raw sensor data into MongoDB for visualization
        collection.insert_one(data)

        print(f"Sent to Kafka and saved to MongoDB: {data}")
        time.sleep(1)  # Send data every second
    except Exception as e:
        print(f"Error in data sending: {e}")
        time.sleep(5)  # Wait 5 seconds before retrying
