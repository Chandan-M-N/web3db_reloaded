import paho.mqtt.client as mqtt
import requests
import json

# MQTT Configuration
MQTT_BROKER = "172.20.24.155"
MQTT_PORT = 1883
MQTT_TOPIC = "topic/#"

# REST API Endpoints
ADD_API = "http://172.20.24.155:5100/add-medical"
FETCH_API = "http://172.20.24.155:5100/fetch-medical"

# Callback when connected to MQTT broker
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    client.subscribe(MQTT_TOPIC)
    print(f"subscribed to {MQTT_TOPIC}")

# Callback when a message is received
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"Message received: Topic={msg.topic}, Payload={payload}")
    print(type(payload))
    print(payload)
    data = json.loads(payload)
    # Call REST API to add data
    response = requests.post(ADD_API, json=data)
    print(f"REST API Response: {response.status_code}, {response.text}")
    

# MQTT Client Setup
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()


#publish command (dont add '' while doing from windows terminal): mosquitto_pub -h 172.20.24.155 -t "topic/bp" -m {\"type\":\"bp\",\"val\":120}
#subscribe command: mosquitto_sub -h 172.20.24.155 -t "topic/bp"
