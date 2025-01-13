import paho.mqtt.client as mqtt

BROKER = "172.20.24.155"
PORT = 1883
TOPICS = [("test/topic", 2), ("test/topic2", 0), ("test/topic3", 0)]  # List of topics and QoS levels
RECEIVED_COUNT = 0

def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    # Subscribe to all topics
    client.subscribe(TOPICS)

def on_message(client, userdata, msg):
    global RECEIVED_COUNT
    RECEIVED_COUNT += 1
    print(f"Received message from {msg.topic}: {msg.payload.decode()}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
