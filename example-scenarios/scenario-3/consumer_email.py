from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='email-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def send_email(order):
    print(f"Sending email for order: {order['order_id']} to user: {order['user_id']}")
    # Simulate email sending
    print(f"Email sent for order: {order['order_id']}")

def consume_orders():
    for message in consumer:
        send_email(message.value)

if __name__ == "__main__":
    consume_orders()
