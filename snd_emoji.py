import requests
import random
from datetime import datetime, timedelta

# Load emojis from emojis.txt
with open("emojis.txt", "r", encoding="utf-8") as file:
    emojis = file.read().splitlines()


def send_emoji_data():
    for i, emoji in enumerate(emojis):
        data = {
            "user_id": f"user{i+1}",
            "emoji_type": emoji,
            "timestamp": datetime.now().isoformat()
        }
        
        response = requests.post('http://localhost:5000/send_emoji', json=data)
        if response.status_code == 200:
            print(f"Sent emoji {emoji} for user {data['user_id']} at {data['timestamp']}")
        else:
            print(f"Failed to send emoji {emoji} for user {data['user_id']}: {response.json()}")

# Run the function
send_emoji_data()

