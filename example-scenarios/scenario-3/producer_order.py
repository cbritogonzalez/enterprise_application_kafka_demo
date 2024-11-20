from kafka import KafkaProducer
import json
import time
import uuid

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_orders():
    orders = [
        {"user_id": "user1", "order_id": str(uuid.uuid4()), "items": ["item1", "item2"], "total": 100},
        {"user_id": "user2", "order_id": str(uuid.uuid4()), "items": ["item3"], "total": 50},
        {"user_id": "user3", "order_id": str(uuid.uuid4()), "items": ["item4", "item5", "item6"], "total": 200},
    ]
    while True:
        for order in orders:
            producer.send('orders-topic', value=order)
            print(f"Produced order: {order}")
            time.sleep(1)

if __name__ == "__main__":
    produce_orders()
