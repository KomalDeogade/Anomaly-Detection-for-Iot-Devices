import pandas as pd
from sklearn.ensemble import IsolationForest
from pymongo import MongoClient
import pickle
import schedule
import time

# Database setup (assuming MongoDB is running via Docker Compose)
client = MongoClient("mongodb://localhost:27017/")
db = client['anomaly_detection']
collection = db['labeled_data']

def fetch_labeled_data():
    # Fetch all labeled feedback entries
    cursor = collection.find({"label": {"$in": ["valid", "normal"]}})
    data = []
    for entry in cursor:
        data.append([
            entry.get("temperature", 0),
            entry.get("pressure", 0),
            entry.get("vibration", 0)
        ])
    return pd.DataFrame(data, columns=["temperature", "pressure", "vibration"])

def retrain_model():
    labeled_data = fetch_labeled_data()
    if len(labeled_data) > 0:
        print(f"Retraining on {len(labeled_data)} samples...")
        model = IsolationForest(n_estimators=100, random_state=42)
        model.fit(labeled_data)
        # Save the model for use by consumer.py
        with open('isoforest_model.pkl', 'wb') as f:
            pickle.dump(model, f)
        print("Retraining complete. Model updated.")
    else:
        print("No new labeled data found. Skipping retraining.")

# ---- Automatically run retraining every 12 hours ----
schedule.every(12).hours.do(retrain_model)

if __name__ == "__main__":
    print("Retraining service started...")
    while True:
        schedule.run_pending()
        time.sleep(60)
