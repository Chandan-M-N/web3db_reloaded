from flask import Flask, request, jsonify
import json
from message_queue.rabbit_queue import send_to_rabbitmq

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