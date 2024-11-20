from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='inventory-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def update_inventory(order):
    print(f"Updating inventory for order: {order['order_id']} (Items: {order['items']})")
    # Simulate inventory update
    print(f"Inventory updated for order: {order['order_id']}")

def consume_orders():
    for message in consumer:
        update_inventory(message.value)

if __name__ == "__main__":
    consume_orders()
