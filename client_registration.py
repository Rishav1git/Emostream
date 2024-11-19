import requests
import threading
import time
from flask import Flask, request, jsonify
import sys

# Check command-line arguments
if len(sys.argv) != 5:
    print("Usage: python client.py <client_port> <client_id> <server_host> <server_port>")
    sys.exit(1)

# Parse command-line arguments
CLIENT_PORT = int(sys.argv[1])
CLIENT_ID = sys.argv[2]
SERVER_HOST = sys.argv[3]
SERVER_PORT = int(sys.argv[4])

app = Flask(__name__)

# Register the client with the subscriber server
def register_client():
    url = f"http://{SERVER_HOST}:{SERVER_PORT}/register_client"
    data = {"client_id": CLIENT_ID, "host": "localhost", "port": CLIENT_PORT}
    try:
        response = requests.post(url, json=data)
        print(response.json())
    except Exception as e:
        print(f"Error registering client: {e}")

# Send periodic heartbeat signals
def send_heartbeat():
    url = f"http://{SERVER_HOST}:{SERVER_PORT}/heartbeat"
    data = {"client_id": CLIENT_ID}
    while True:
        try:
            response = requests.post(url, json=data)
            print(f"Heartbeat response: {response.json()}")
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
        time.sleep(5)  # Heartbeat interval

# Start the HTTP server to receive data from the subscriber
@app.route('/receive_data', methods=['POST'])
def receive_data():
    try:
        data = request.json.get('data')
        print(f"Received data: {data}")
        return jsonify({"message": "Data received successfully"}), 200
    except Exception as e:
        print(f"Error receiving data: {e}")
        return jsonify({"error": "Failed to process received data"}), 500

def start_client():
    # Start the heartbeat in a separate thread
    threading.Thread(target=send_heartbeat, daemon=True).start()
    # Start the Flask server to listen for data
    app.run(host="0.0.0.0", port=CLIENT_PORT)

if __name__ == "__main__":
    register_client()
    start_client()

