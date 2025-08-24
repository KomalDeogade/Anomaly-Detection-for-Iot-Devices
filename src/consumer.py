from kafka import KafkaConsumer
import json
import pandas as pd
from sklearn.ensemble import IsolationForest
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['anomaly_detection']
collection = db['anomaly_results']

normal_data = pd.DataFrame({
    "temperature": [30, 32, 29, 31, 35, 29, 33, 31],
    "pressure": [5.0, 5.2, 4.9, 5.1, 5.0, 5.1, 5.3, 4.8],
    "vibration": [0.12, 0.14, 0.09, 0.13, 0.11, 0.13, 0.10, 0.12]
})

model = IsolationForest(contamination=0.05, n_estimators=100, random_state=42)
model.fit(normal_data)

consumer = KafkaConsumer(
    'iot-sensor-data',
    bootstrap_servers=['127.0.0.1:9092'],
    api_version=(2, 0, 0),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

for message in consumer:
    data = message.value
    features = [[
        data.get('temperature', 0),
        data.get('pressure', 0),
        data.get('vibration', 0)
    ]]
    prediction = model.predict(features)
    is_anomaly = prediction[0] == -1

    print(f"Received: {data} | Anomaly: {is_anomaly}")

    try:
        collection.update_one(
            {"timestamp": data["timestamp"]},
            {"$set": {"is_anomaly": is_anomaly}}
        )
    except Exception as e:
        print(f"MongoDB update error: {e}")
