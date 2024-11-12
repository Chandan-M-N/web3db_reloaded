import subprocess
from flask import Flask, request, jsonify
from postgres import pos_cl
app = Flask(__name__)

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

if __name__ == '__main__':
    app.run(debug=True)
