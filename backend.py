from flask import Flask, request, jsonify
import json
from message_queue.rabbit_queue import send_to_rabbitmq
from database import db_operations as db
from ipfs_content import ipfs_operations as ipfs
import os

app = Flask(__name__)

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


@app.route('/get-medical', methods=['GET'])
def get_medical_data():
    try:
        # Get the JSON data from the request
        data = request.json  # This is unconventional for GET, but will work if body is sent
        
        # Extract time or points from the body
        if not data:
            return jsonify({"status": "error", "message": "No data provided in the request body"}), 400
        
        if 'time' in data and 'topic' in data:
            time = data['time']
            topic = data['topic']
        else:
            return jsonify({"status": "error", "message": "Request must contain 'time' and 'topic'"}), 400

        # Fetch CIDs for the given time period
        hash_list,message = db.fetch_cids_by_time(time,topic)

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



@app.route('/device-list', methods=['POST'])
def devices():
    data = request.get_json()  # Get JSON data from the request
    eth_address = data.get("ethereum_address")  # Extract Ethereum address
    device_ids = data.get("device_ids")  # Extract list of device IDs

    print(f"Ethereum Address: {eth_address}")
    print(f"Device IDs: {device_ids}")

    return jsonify({"message": "Data received successfully"}), 200


@app.route('/subscribe-devices', methods=['POST'])
def subscribe_devices():
    data = request.get_json()  # Get JSON data from the request
    subscriber_eth_address = data.get("subscriber_ethereum_address")  # Extract subscriber's Ethereum address
    subscribe_device_ids = data.get("subscribe_device_ids")  # Extract list of devices to be subscribed

    print(f"Subscriber Ethereum Address: {subscriber_eth_address}")
    print(f"Devices to Subscribe: {subscribe_device_ids}")

    return jsonify({"message": "Subscription data received successfully"}), 200


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)