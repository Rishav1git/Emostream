from kafka import KafkaConsumer, KafkaProducer
import json
import time

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Kafka consumer to consume from the processed topic
consumer = KafkaConsumer(
    'processed-emoji-data',  # Ensure this topic matches your processed data output from spark.py
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def publish_to_clusters(data):
    # Publish data to each cluster topic
    for cluster_id in range(1, 4):  # Three clusters: cluster_1, cluster_2, cluster_3
        topic = f'cluster_{cluster_id}'
        print(f"Publishing to topic {topic}: {data}")  # Debugging output
        producer.send(topic, value=data)
    producer.flush()

if __name__ == "__main__":
    print("Starting main_publisher.py...")  # Debugging output
    try:
        for message in consumer:
            data = message.value
            print(f"Received data from 'processed-emoji-data': {data}")  # Debugging output
            
            # Send data to the clusters
            publish_to_clusters(data)
            print(f"Published data to all clusters: {data}")  # Debugging output
            time.sleep(1)  # Optional, to simulate a delay between publishing

    except Exception as e:
        print(f"Error encountered: {e}")  # Debugging output for error handling
