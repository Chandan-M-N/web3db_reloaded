from flask import Flask, request, jsonify
import json
from message_queue.rabbit_queue import send_to_rabbitmq
from database import db_operations as db
from ipfs_content import ipfs_operations as ipfs
import os
from flask_cors import CORS
import hashlib

app = Flask(__name__)
CORS(app)


def hash_wallet_id(wallet_id):
    """
    Hash the wallet_id using SHA-256.
    """
    return hashlib.sha256(wallet_id.encode()).hexdigest()

@app.route('/add-medical', methods=['POST'])
def medical_data():
    try:
        # Get the JSON data from the request
        data = request.json

        # Extract the topic and payload
        topic = data.get('topic')
        payload = data.get('payload')

        print(f"Topic: {topic}")
        print(f"Payload: {payload}")

        # If the payload is a string, parse it as JSON
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError as e:
                print(f"Error parsing payload: {e}")
                return jsonify({"status": "error", "message": "Invalid JSON payload"}), 400

        # Print the parsed payload
        print("Parsed Payload:")
        print(json.dumps(payload, indent=2))

        # Prepare the message to send to RabbitMQ
        message = {
            "topic": topic,
            "payload": payload
        }
        print('Sending message to RabbitMQ...')
        send_to_rabbitmq(message)
        print('Message sent successfully!')

        # Return a success response
        return jsonify({"status": "success", "message": "Data received and sent to RabbitMQ"}), 200

    except Exception as e:
        print(f"Error in medical_data: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get-medical', methods=['POST'])
def get_medical_data():
    try:
        # Get the JSON data from the request
        data = request.json  # This is unconventional for GET, but will work if body is sent
        
        # Extract time or points from the body
        if not data:
            return jsonify({"status": "error", "message": "No data provided in the request body"}), 400
        
        if 'time' in data and 'topic' in data and 'date' in data:
            time = data['time']
            topic = data['topic']
            date = data['date']
        else:
            return jsonify({"status": "error", "message": "Request must contain 'time', 'topic', 'date' and 'wallet_id'"}), 400

        # Fetch CIDs for the given time period
        hash_list,message = db.fetch_cids_by_time(date,time,topic)
        if hash_list:
            # Fetch files from IPFS and store them in dumps/{cid}.json
            for cid in hash_list:
                ipfs.fetch_file_from_ipfs_cluster(cid)
            
            # Read all JSON files from the dumps directory and merge their contents
            merged_data = []
            for cid in hash_list:
                file_path = f"dumps/{cid}.json"
                if os.path.exists(file_path):
                    try:
                        # Read the file and add its contents to the list
                        with open(file_path, 'r') as file:
                            file_data = json.load(file)
                            merged_data.append(file_data)
                        
                        # Remove the file after processing
                        os.remove(file_path)
                    except Exception as e:
                        print(f"Error processing or removing file {file_path}: {e}")
                        continue

            # Return the merged data as a JSON response
            return jsonify({"data": merged_data}), 200
        else:
            # Return a response if no CIDs are found
            return jsonify({"status": "success", "message": f"{message}"}), 200

    except Exception as e:
        print(f"Error in get_medical_data: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route('/add-device', methods=['POST'])
def devices():
    # Get JSON data from the request
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided in the request body"}), 400

    # Extract wallet_id and device_id from the JSON data
    wallet_id = data.get("wallet_id", None)
    device_id = data.get("device_id", None)

    # Validate wallet_id and device_id
    if wallet_id is None:
        return jsonify({"status": "error", "message": "No wallet_id provided in the request body"}), 400
    if device_id is None:
        return jsonify({"status": "error", "message": "No device_id provided in the request body"}), 400

    # Split device_id into a list of individual device IDs if it contains commas
    device_ids = [id.strip() for id in device_id.split(",")] if device_id else []

    # Print wallet_id and device_ids for debugging
    print(f"Wallet ID: {wallet_id}")
    print(f"Device IDs: {device_ids}")
    output = db.add_device(wallet_id,device_ids)
    # Return a success response
    return jsonify(output), 200


@app.route('/subscribe-device', methods=['POST'])
def subscribe_devices():
    data = request.get_json()  # Get JSON data from the request
    if not data:
        return jsonify({"status": "error", "message": "No data provided in the request body"}), 400
    
    subscriber_eth_address = data.get("subscriber_ID",None)  # Extract subscriber's Ethereum address
    subscribe_device_ids = data.get("device_id",None)  # Extract list of devices to be subscribed

    if subscriber_eth_address is None:
        return jsonify({"status": "error", "message": "No subscriber_ID provided in the request body"}), 400
    if subscribe_device_ids is None:
        return jsonify({"status": "error", "message": "No device_id provided in the request body"}), 400

    print(f"Subscriber Address: {subscriber_eth_address}")
    print(f"Devices to Subscribe: {subscribe_device_ids}")

    subs_id = hash_wallet_id(subscriber_eth_address)
    #add policy to contract
    return jsonify({"message": "Subscription data received successfully"}), 200


@app.route('/access-device', methods=['POST'])
def access_subs_device():
    data = request.get_json()  # Get JSON data from the request
    if not data:
        return jsonify({"status": "error", "message": "No data provided in the request body"}), 400

    if 'time' in data and 'topic' in data and 'date' in data and 'wallet_id' in data:
            time = data['time']
            topic = data['topic']
            date = data['date']
            wallet_id = data['wallet_id']
    else:
        return jsonify({"status": "error", "message": "Request must contain 'time', 'topic', 'date' and 'wallet_id'"}), 400

    subs_id = hash_wallet_id(wallet_id)
    if wallet_id in ['abcd12345']:
        policy = True
    else:
        policy = False
    #check policy in contract
    if not policy:
        return jsonify({"message": "Access denied"}), 403

    # Fetch CIDs for the given time period
    hash_list,message = db.fetch_cids_by_time(date,time,topic)
    if hash_list:
        # Fetch files from IPFS and store them in dumps/{cid}.json
        for cid in hash_list:
            ipfs.fetch_file_from_ipfs_cluster(cid)
        
        # Read all JSON files from the dumps directory and merge their contents
        merged_data = []
        for cid in hash_list:
            file_path = f"dumps/{cid}.json"
            if os.path.exists(file_path):
                try:
                    # Read the file and add its contents to the list
                    with open(file_path, 'r') as file:
                        file_data = json.load(file)
                        merged_data.append(file_data)
                    
                    # Remove the file after processing
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error processing or removing file {file_path}: {e}")
                    continue

        # Return the merged data as a JSON response
        return jsonify({"data": merged_data}), 200
    else:
        # Return a response if no CIDs are found
        return jsonify({"status": "success", "message": f"{message}"}), 200

    


@app.route('/get-registered-devices',methods=['POST'])
def get_device_list():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided in the request body"}), 400
    
    if 'wallet_id' not in data:
        return jsonify({"status": "error", "message": "No wallet_id provided in the request body"}), 400
    
    wallet_id = data['wallet_id']

    hashed_wallet_id = hash_wallet_id(wallet_id)

    result = db.get_user_devices(hashed_wallet_id)
    return jsonify({"devices": result}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5100, debug=True)
