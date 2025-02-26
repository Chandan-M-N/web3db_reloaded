import paho.mqtt.client as mqtt
import time

# MQTT Broker details
broker = "0.0.0.0"
port = 1883  # Default MQTT port
topic = "topic/shsjdkkkddh/djdhdjjd"


# Convert payload to JSON string
import json


# Create MQTT client and connect
client = mqtt.Client()
client.connect(broker, port, 60)


payload = {
"time": "2024-01-31T12:00:00Z",
"data": {
    "heart_rate": 74,
    "blood_pressure": {"sys": 120, "dia": 80}
}
}

# Publish message
message = json.dumps(payload)
client.publish(topic, message)
print(f"Published to {topic}: {message}")
time.sleep(0.5)

# Disconnect client
client.disconnect()
