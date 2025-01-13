import subprocess
from flask import Flask, request, jsonify
from postgres import pos_cl
from med_data import med
from flask_cors import CORS
import json
app = Flask(__name__)
CORS(app)

@app.route('/query', methods=['POST'])
def process_string():
    data = request.get_json()  # Get JSON data from the request
    input_query = data.get('query', '')

    if not input_query:
        return jsonify({"error": "No SQL query provided"}), 400

    # Run the subprocess using shell=True to allow pipes
    result = subprocess.run(
        f"echo \"{input_query}\" | pgsanity",
        capture_output=True,
        text=True,
        shell=True
    )
    # Check if pgsanity returned an error message (syntax error)
    if result.returncode != 0:
        return jsonify({"output": result.stdout + result.stderr}) 
    
    op, msg = pos_cl.handle_query(input_query)
    if op == True:
        return jsonify({"output": msg})
    else:
        msg = str(msg)
        return jsonify({"error": msg})
    

    
@app.route('/add-medical', methods=['POST'])
def medical_data():
    try:
        # Parse the incoming JSON data
        data = request.json
        if 'publish_received_at' in data:
            payload = data['payload']
            data = json.loads(payload)
        # Extract the type of medical data
        if 'type' not in data:
            return jsonify({"error": "Please add type key to specify type of data"})
        medical_type = data.pop('type', None) 
        print(medical_type)
        op = med.check_table_exists(medical_type.lower(),data,"sunlab")
        return jsonify({"val": op})
    
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500

@app.route('/fetch-medical', methods=['POST'])
def fetch_medical_data():
    try:
        # Parse the incoming JSON data
        data = request.json
        
        # Extract the type of medical data
        if 'type' not in data:
            return jsonify({"error": "Please add type key to specify type of data"})

        medical_type = data.pop('type', None) 
        json_data = med.fetch_data_api(medical_type.lower())
        print(json_data)
        return jsonify(json_data)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5100,debug=True)

