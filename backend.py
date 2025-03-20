from flask import Flask, request, jsonify
import json
from message_queue.rabbit_queue import send_to_rabbitmq
from database import db_operations as db
from ipfs_content import ipfs_operations as ipfs
import os
from flask_cors import CORS
import hashlib
from access_control.access_control import AccessControl


try:
    access_control = AccessControl()
except Exception as e:
    access_control = None
    print(f"Error while setting up access control {e}")

app = Flask(__name__)
CORS(app)


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
    name = data.get("name", None)
    category = data.get("category",None)
    measurement_units = data.get("measurement_unit",None)


    # Validate wallet_id and device_id
    if wallet_id is None:
        return jsonify({"status": "error", "message": "No wallet_id provided in the request body"}), 400
    if device_id is None:
        return jsonify({"status": "error", "message": "No device_id provided in the request body"}), 400
    if category is None or measurement_units is None or name is None:
        return jsonify({"status": "error", "message": "Please provide category, measurement_units and name in the request body"}), 400

    # Split device_id into a list of individual device IDs if it contains commas
    device_ids = [id.strip() for id in device_id.split(",")] if device_id else []
    category_list = [id.strip() for id in category.split(",")] if category else []
    measurement_units_list = [id.strip() for id in measurement_units.split(",")] if measurement_units else []
    names = [id.strip() for id in name.split(",")] if name else []

    # Print wallet_id and device_ids for debugging
    print(f"Wallet ID: {wallet_id}")
    print(f"Device IDs: {device_ids}")
    print(f"Categories: {category_list}")
    print(f"Measurement Units: {measurement_units_list}")
    print(f"Names: {names}")

    if len(device_ids) != len(category_list):
        return jsonify({"status": "error", "message": "The number of categories does not match the number of devices"}), 400
    if len(device_ids) != len(measurement_units_list):
        return jsonify({"status": "error", "message": "The number of measurement units does not match the number of devices"}), 400
    if len(device_ids) != len(names):
        return jsonify({"status": "error", "message": "The number of names does not match the number of devices"}), 400

    output = db.add_device(wallet_id,device_ids,names,category_list,measurement_units_list)
    # Return a success response
    return jsonify(output), 200


@app.route('/share-access', methods=['POST'])
def subscribe_devices():
    data = request.get_json()  # Get JSON data from the request
    if not data:
        return jsonify({"status": "error", "message": "No data provided in the request body"}), 400
    
    device_owner = data.get("owner_id",None)
    subscriber_email = data.get("subscriber_email",None)  # Extract subscriber's Ethereum address
    subscribe_device_ids = data.get("device_id",None)  # Extract list of devices to be subscribed

    if device_owner is None:
        return jsonify({"status": "error", "message": "No owner_id provided in the request body"}), 400
    if subscriber_email is None:
        return jsonify({"status": "error", "message": "No subscriber_email provided in the request body"}), 400
    if subscribe_device_ids is None:
        return jsonify({"status": "error", "message": "No device_id provided in the request body"}), 400

    print(f"Subscriber Address: {subscriber_email}")
    print(f"Devices to Subscribe: {subscribe_device_ids}")


    row = db.check_device_exists(device_owner, subscribe_device_ids)
    if row is False:
        return jsonify({"message": "Device does not exist in owner's list"}), 400
    
    wallet_id = db.get_wallet_id_by_email(subscriber_email)
    if wallet_id is None:
        return jsonify({"message": "No data found for given email"}), 200

    #add policy to contract
    if access_control is None:
        return jsonify({"message": "Failed to connect to Ethereum network"}), 200

    op,message = access_control.add_policy(subscribe_device_ids,wallet_id)

    if op is False:
        return jsonify({"status": "Failed","message":message}), 200
    return jsonify({"status": "success","message":message}), 200


@app.route('/evaluate-policy', methods=['POST'])
def check_policy():
    data = request.get_json()  # Get JSON data from the request
    if not data:
        return jsonify({"status": "error", "message": "No data provided in the request body"}), 400
    
    subscriber_eth_address = data.get("subscriber_wallet_id",None)  # Extract subscriber's Ethereum address
    subscriber_device_id = data.get("device_id",None)  # Extract list of devices to be subscribed
    
    if subscriber_eth_address is None:
        return jsonify({"status": "error", "message": "No subscriber_wallet_id provided in the request body"}), 400
    if subscriber_device_id is None:
        return jsonify({"status": "error", "message": "No device_id provided in the request body"}), 400

    print(f"Subscriber Address: {subscriber_eth_address}")
    print(f"Devices to Subscribe: {subscriber_device_id}")
    
    #check policy
    if access_control is None:
        return jsonify({"message": "Failed to connect to Ethereum network"}), 200
    
    op,message= access_control.evaluate_policy(subscriber_device_id,subscriber_eth_address)

    if op is False:
        return jsonify({"status": "Failed","message":message}), 403
    return jsonify({"status": "success","message":message}), 200



@app.route('/get-registered-devices',methods=['POST'])
def get_device_list():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided in the request body"}), 400
    
    if 'wallet_id' not in data:
        return jsonify({"status": "error", "message": "No wallet_id provided in the request body"}), 400
    
    wallet_id = data['wallet_id']

    result = db.get_user_devices(wallet_id)
    return jsonify({"devices": result}), 200


@app.route('/add-profile', methods=['POST'])
def profile():
    try:
        # Get JSON data from the request
        data = request.get_json()

        # Check if all required fields are present
        required_fields = ['wallet_id', 'name', 'email', 'height', 'weight', 'age', 'gender', 'bmi']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Extract data from JSON
        wallet_id = data['wallet_id']
        name = data['name']
        email = data['email']
        height = data['height']
        weight = data['weight']
        age = data['age']
        gender = data['gender']
        bmi = data['bmi']

        op = db.add_user_profile(wallet_id,name,email,height,weight,age,gender,bmi)
        if op:
            return jsonify({"message": "success"}), 200
        return jsonify({"message": "failed"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get-profile',methods=['POST'])
def fetch_profile():
    data = request.get_json()
    if 'wallet_id' not in data:
        return jsonify({"error": f"Missing required field wallet_id"}), 400
    
    wallet_id = data['wallet_id']

    op = db.get_user_profile_by_wallet_id(wallet_id)

    if op is None:
        return jsonify({"message": "No data found"}), 200
    return jsonify(op),200



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5100, debug=True)
