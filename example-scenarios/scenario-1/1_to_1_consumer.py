from kafka import KafkaConsumer # type: ignore

#name of the topic
KAFKA_TOPIC = "test_kafka_basic"

# set up the Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers='localhost:9094',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group')

print("Waiting for messages...")

# listen for messages
try:
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
