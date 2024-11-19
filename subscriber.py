from kafka import KafkaConsumer
from flask import Flask, request, jsonify
import json
import logging
import threading
import time
import sys
import requests

# Check command-line arguments
if len(sys.argv) != 4:
    print("Usage: python subscriber.py <cluster_id> <subscriber_id> <port>")
    sys.exit(1)

# Parse command-line arguments
CLUSTER_ID = sys.argv[1]
SUBSCRIBER_ID = sys.argv[2]
PORT = int(sys.argv[3])  # Port number
TOPIC = f"cluster_{CLUSTER_ID}"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)

# Global variables
registered_clients = {}  # Key: client_id, Value: {host, port, last_heartbeat_time}
HEARTBEAT_TIMEOUT = 10  # Timeout in seconds to deregister inactive clients

# Kafka Consumer setup
try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
except Exception as e:
    logging.error(f"Failed to initialize KafkaConsumer: {e}")
    exit(1)

def broadcast_to_clients(data):
    """Send data to all registered clients."""
    for client_id, client_details in list(registered_clients.items()):
        try:
            url = f"http://{client_details['host']}:{client_details['port']}/receive_data"
            response = requests.post(url, json={"data": data})
            if response.status_code != 200:
                logging.warning(f"Failed to send data to {client_id}. Status: {response.status_code}")
        except Exception as e:
            logging.error(f"Error sending data to client {client_id}: {e}")

@app.route("/register_client", methods=["POST"])
def register_client():
    """Register a client to receive data."""
    client_data = request.json
    client_id = client_data.get("client_id")
    client_host = client_data.get("host")
    client_port = client_data.get("port")

    if not client_id or not client_host or not client_port:
        return jsonify({"error": "Missing client_id, host, or port"}), 400

    registered_clients[client_id] = {
        "host": client_host,
        "port": client_port,
        "last_heartbeat_time": time.time(),
    }
    logging.info(f"Registered client {client_id}: {client_host}:{client_port}")
    return jsonify({"message": f"Client {client_id} registered successfully."}), 200

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    """Update the heartbeat timestamp for a client."""
    client_data = request.json
    client_id = client_data.get("client_id")

    if client_id in registered_clients:
        registered_clients[client_id]["last_heartbeat_time"] = time.time()
        return jsonify({"message": f"Heartbeat received from {client_id}."}), 200
    else:
        return jsonify({"error": "Client not registered."}), 404

def kafka_listener():
    """Listen to Kafka topic and broadcast data to clients."""
    logging.info(f"Subscriber {SUBSCRIBER_ID} listening to Kafka topic {TOPIC}.")
    try:
        for message in consumer:
            data = message.value
            broadcast_to_clients(data)
    except Exception as e:
        logging.error(f"Error while consuming messages from Kafka: {e}")

def monitor_heartbeats():
    """Deregister clients that have not sent a heartbeat within the timeout period."""
    while True:
        current_time = time.time()
        for client_id in list(registered_clients.keys()):
            if current_time - registered_clients[client_id]["last_heartbeat_time"] > HEARTBEAT_TIMEOUT:
                logging.warning(f"Deregistering client {client_id} due to heartbeat timeout.")
                del registered_clients[client_id]
        time.sleep(HEARTBEAT_TIMEOUT // 2)

if __name__ == "__main__":
    # Start Kafka listener in a separate thread
    threading.Thread(target=kafka_listener, daemon=True).start()
    # Start heartbeat monitor in a separate thread
    threading.Thread(target=monitor_heartbeats, daemon=True).start()
    # Start Flask server for client management
    app.run(host="0.0.0.0", port=PORT)

