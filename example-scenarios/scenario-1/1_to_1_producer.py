from kafka import KafkaProducer # type: ignore
import time

#name of the topic
KAFKA_TOPIC = "test_kafka_basic"

# Set up the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9094')

nr = 0

# send a message every 7 seconds
try:
    while True:
        message = f"test whether Kafka 1 to 1 works, iteration {nr}"
        nr += nr + 1
        producer.send(KAFKA_TOPIC, value=message.encode('utf-8'))
        print(f"Sent message: {message}")
        time.sleep(3)
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    producer.close()
