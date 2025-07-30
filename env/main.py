import requests
import json
from quixstreams import Application

# Fetch hourly weather forecast
response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=35.6895&longitude=139.6917&hourly=temperature_2m")
weather = response.json()

# Set up Kafka producer app
app = Application(
    broker_address="192.168.56.1:9092",
    loglevel="DEBUG",
)

with app.get_producer() as producer:
    # Limit payload size to avoid Kafka size limits
    temperature_data = weather.get("hourly", {}).get("temperature_2m", [])[:5]  # First 5 entries
    producer.produce(
        topic="weather",
        key="tokyo",
        value=json.dumps(temperature_data),
    )
