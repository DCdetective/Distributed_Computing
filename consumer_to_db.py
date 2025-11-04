from kafka import KafkaConsumer
import json
import sqlite3
import time

# SQLite connection
conn = sqlite3.connect("iot_data.db")
cur = conn.cursor()

# Create table if not exists
cur.execute("""
CREATE TABLE IF NOT EXISTS sensor_readings (
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL
)
""")
conn.commit()

consumer = KafkaConsumer(
    'iot-sensors',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("📥 Kafka Consumer Started...")

for msg in consumer:
    data = msg.value
    cur.execute("INSERT INTO sensor_readings VALUES (?, ?, ?, ?)", 
                (data["sensor_id"], data["timestamp"], data["temperature"], data["humidity"]))
    conn.commit()
    print("Inserted:", data)
