from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import time

app = Flask(__name__)

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# API endpoint to receive emoji data
@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    try:
        data = request.get_json()
        
        # Validate data (basic check)
        if not all(key in data for key in ("user_id", "emoji_type", "timestamp")):
            return jsonify({"error": "Invalid data format"}), 400

        # Send data to Kafka topic
        producer.send('emoji-events', value=data)
        producer.flush(timeout=0.5)  # Flush every 500ms

        print(f"Emoji data sent to Kafka: {data}")
        return jsonify({"message": "Emoji received"}), 200

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)

