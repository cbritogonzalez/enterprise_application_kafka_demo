from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='payment-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def process_payment(order):
    print(f"Processing payment for order: {order['order_id']} (Total: {order['total']})")
    # Simulate payment success
    print(f"Payment successful for order: {order['order_id']}")

def consume_orders():
    for message in consumer:
        process_payment(message.value)

if __name__ == "__main__":
    consume_orders()
