from kafka import KafkaConsumer
import json
import sys
import logging
import signal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read cluster_id from command-line arguments
if len(sys.argv) != 2:
    logging.error("Usage: python cluster_publisher.py <cluster_id>")
    sys.exit(1)

cluster_id = sys.argv[1]
topic = f'cluster_{cluster_id}'

# Initialize Kafka consumer for the specified cluster topic
try:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
except Exception as e:
    logging.error(f"Failed to initialize KafkaConsumer: {e}")
    sys.exit(1)

# Notify subscribers (simulated for now)
def notify_subscribers(data):
    # Simulate notifying three subscribers in the cluster
    for subscriber_id in range(1, 4):  # Replace with real logic if needed
        logging.info(f"Forwarding data to Subscriber {subscriber_id} in Cluster {cluster_id}: {data}")

# Graceful shutdown handler
def handle_shutdown(signal, frame):
    logging.info("Shutting down Cluster Publisher...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_shutdown)

def listen_to_main_publisher():
    logging.info(f"Cluster Publisher {cluster_id}: Listening on topic {topic}...")
    try:
        for message in consumer:
            data = message.value
            logging.info(f"Cluster {cluster_id} received data: {data}")
            # Forward data to subscribers
            notify_subscribers(data)
    except Exception as e:
        logging.error(f"Error while listening to topic {topic}: {e}")

if __name__ == "__main__":
    listen_to_main_publisher()
