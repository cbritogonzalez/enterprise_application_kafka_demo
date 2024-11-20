import json
import time
from api import fetch_streaming_weather
from kafka import KafkaProducer

#name of the topic
STREAMING_KAFKA_TOPIC = "streaming_weather_data"
STREAMING_LIMIT = 5
keyFile = open('api_key.txt', 'r')
_TOKEN = keyFile.readline() #from api.weatherapi.com

#server that the producer will be running
producer = KafkaProducer(bootstrap_servers='localhost:9094')


#send streaming weather data to the consumer
for i in range(1, STREAMING_LIMIT):
    streaming_data = fetch_streaming_weather(_TOKEN)

    location = streaming_data['location']
    current = streaming_data['current']
        
    streaming_weather_data = {
        'name': location['name'],
        'localtime': location['localtime'],
        'last_update': current['last_updated'],
        'temp_c': current['temp_c'],
        'feelslike_c': current['feelslike_c']
    }

    producer.send(STREAMING_KAFKA_TOPIC, json.dumps(streaming_weather_data).encode("utf-8"))
    print(streaming_weather_data)

    print(f"Done Sending..{i}")
    time.sleep(5)


