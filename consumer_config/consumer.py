import json
import pika
import os
import random
import string
from ipfs_content import ipfs_operations as ipfs

def generate_random_filename():
    """Generates a random 46-character filename for the JSON file."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=46)) + ".json"

def save_payload_to_file(payload):
    """Saves the payload to a JSON file in the dumps folder."""
    os.makedirs("dumps", exist_ok=True)
    filename = os.path.join("dumps", generate_random_filename())
    with open(filename, "w") as file:
        json.dump(payload, file, indent=4)
    print(f" [x] Payload saved to {filename}")
    return filename

def process_message(ch, method, properties, body):
    print("Received message:", body)
    message = json.loads(body)
    if message.get('request') == 'post':
    
        topic = message.get("topic", "Unknown")
        payload = message.get("payload", {})

        print(f" [x] Topic: {topic}")
        print(f" [x] Payload: {payload}")

        filename = save_payload_to_file(payload)
        ipfs_output = ipfs.add_file_with_metadata(filename)
        
        print(f"IPFS output: {ipfs_output}")  # Debug print to check the output
        
        if ipfs_output[0] != True:
            print("IPFS output not True, skipping ACK")  # Debug statement to understand why we're skipping ACK
            return  # Do not acknowledge; message will be requeued for retry
        
        print('ack')
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    elif message.get('request') == 'get':
        print(body)
        pass


def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.basic_consume(queue='medical_data_queue', on_message_callback=process_message, auto_ack=False)
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Consumer stopped.")
        connection.close()

if __name__ == "__main__":
    start_consumer()
