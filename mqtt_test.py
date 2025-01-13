import paho.mqtt.client as mqtt
import time

BROKER = "172.20.24.155"
PORT = 1883
TOPIC = "test/topic"
MESSAGE = "Test message"
MESSAGE_RATE = 1000  # Messages per second

def main():
    client = mqtt.Client()
    client.connect(BROKER, PORT, 60)
    client.loop_start()
    count = 0
    try:
        start_time = time.time()
        while True:
            client.publish(TOPIC, f"{MESSAGE} {count}")
            count += 1
            if count % MESSAGE_RATE == 0:
                time.sleep(1)  # Adjust for the message rate
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()
        print(f"Sent {count} messages.")
        end_time = time.time()
        print(f"Duration: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
