import json
import pika
import os
import random
import string
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ipfs_content import ipfs_operations as ipfs
from database import db_operations as db

def generate_random_filename():
    """Generates a random 46-character filename for the JSON file."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=46)) + ".json"

def save_payload_to_file(payload):
    """Saves the payload to a JSON file in the dumps folder."""
    try: 
        os.makedirs("dumps", exist_ok=True)
        filename = os.path.join("dumps", generate_random_filename())
        with open(filename, "w") as file:
            json.dump(payload, file, indent=4)
        print(f" [x] Payload saved to {filename}")
        return filename
    except Exception as e:
        print(f"Failed {e}")
        return False

def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        print(f"Failed to delete file {filename} {e}")

def process_message(ch, method, properties, body):
    print("Received message:", body)
    message = json.loads(body)
    
    topic = message.get("topic", "Unknown")
    payload = message.get("payload", {})

    print(f" [x] Topic: {topic}")
    print(f" [x] Payload: {payload}")

    filename = save_payload_to_file(payload)
    if filename == False:
        print("Failed to add payload to json file")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    ipfs_output = ipfs.add_file_with_metadata(filename)
    
    silentremove(filename)

    if ipfs_output[0] != True:
        print("IPFS output not True")
        ch.basic_ack(delivery_tag=method.delivery_tag)  
        return 
    cid = ipfs_output[1]
    db_output = db.add_hash(cid,topic)
    if db_output == False:
        print(f"Adding CID to database failed, {topic},{payload},{cid}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return
    

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
