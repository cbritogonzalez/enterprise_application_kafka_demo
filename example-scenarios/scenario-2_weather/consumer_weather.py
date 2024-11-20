import json

from kafka import KafkaConsumer

STREAMING_CONFIRMED_KAFKA_TOPIC = "streaming_confirmed"
STREAMING_KAFKA_TOPIC = "streaming_weather_data"
HISTORICAL_KAFKA_TOPIC = "historical_weather_data"

consumer_streaming = KafkaConsumer(
    STREAMING_KAFKA_TOPIC,
    bootstrap_servers='localhost:9094'
)

print("Consumer streaming listening...")
while True:
    for message in consumer_streaming:
        print("Ongoing streaming transaction")
        consumed_message = json.loads(message.value.decode())

        city = consumed_message['name']
        local_time = consumed_message['localtime']
        temperature_c = consumed_message["temp_c"]


        print("Successfull transaction")
        print(f"streaming weather data from {city}. At time {local_time} temp {temperature_c} Celsius.")
