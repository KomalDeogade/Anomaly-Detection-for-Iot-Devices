from kafka import KafkaConsumer
import pandas as pd
import json
from sklearn.ensemble import IsolationForest
import pickle
from pymongo import MongoClient
import os

# Set up MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client['anomaly_detection']
collection = db['anomaly_results']

# Load or train model
MODEL_PATH = 'isoforest_model.pkl'

if os.path.exists(MODEL_PATH):
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    print("Loaded pre-trained Isolation Forest model.")
else:
    # Train a demo model on dummy normal data
    X_train = pd.DataFrame({
        "temperature": [30, 32, 31, 29, 35],
        "pressure": [5, 5.2, 4.9, 5.1, 5],
        "vibration": [0.1, 0.15, 0.13, 0.09, 0.14]
    })
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X_train)
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)
    print("Trained and saved a new Isolation Forest model.")

# Kafka consumer setup
consumer = KafkaConsumer(
    'iot-sensor-data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

for msg in consumer:
    data = msg.value
    features = [[
        data.get("temperature", 0),
        data.get("pressure", 0),
        data.get("vibration", 0)
    ]]
    prediction = model.predict(features)
    is_anomaly = prediction[0] == -1

    print("Anomaly" if is_anomaly else "Normal", data)

    # Update MongoDB record with anomaly flag
    try:
        collection.update_one(
            {"timestamp": data["timestamp"]},
            {"$set": {"is_anomaly": is_anomaly}},
            upsert=True  # create if not exists
        )
    except Exception as e:
        print(f"Error updating MongoDB: {e}")
            