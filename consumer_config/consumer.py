import json
import pika
import os
import random
import string
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ipfs_content import ipfs_operations as ipfs
from database import db_operations as db
from encryption import encrypt


def symmetric_key(wallet_id):
    try:
        key = db.get_encryption_key(wallet_id)
        if key is None:
            key, _ = encrypt.generate_symmetric_key()
            db.add_encryption_key(wallet_id,key)
        return key
    except Exception as e:
        print(e)
        return None

def process_message(ch, method, properties, body):
    try: 
        print("Received message:", body)
        message = json.loads(body)
        
        topic = message.get("topic", "Unknown")
        payload = message.get("payload", {})

        print(f" [x] Topic: {topic}")
        print(f" [x] Payload: {payload}")

        wallet_id = db.get_wallet_id_by_device(topic)

        if wallet_id is None:
            print(f"{topic} not registered under any user")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return 
        
        key = symmetric_key(wallet_id)
        if key is None:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return 
        
        encrypted_data = encrypt.encrypt_json_string(payload,key)

        ipfs_output,cid = ipfs.add_file_with_metadata(encrypted_data)
        
        if ipfs_output != True:
            print(f"Failed to add file to IPFS, {cid}")
            ch.basic_ack(delivery_tag=method.delivery_tag)  
            return 
        
        db_output = db.add_hash(cid,topic)
        if db_output == False:
            print(f"Adding CID to database failed, {topic},{payload},{cid}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    except Exception as e:
        print(e)
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
