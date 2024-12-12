from kafka import KafkaProducer
import json
import time
from src.api.fetch_data import fetch_data

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization of data
)

# Function to send data to Kafka
def send_to_kafka(data, topic):
    producer.send(topic, data)
    print(f"Data sent to Kafka on topic '{topic}'.")

# Main function to retrieve data from fetch_data.py and send to Kafka
def produce_data():
    # Retrieve real-time data from fetch_data.py
    ohlcv_data, order_book, recent_trades = fetch_data()

    # Send data to Kafka (topic 'stock_prices')
    if ohlcv_data:
        send_to_kafka(ohlcv_data, 'stock_prices')
    
    if order_book:
        send_to_kafka(order_book, 'stock_prices')
    
    if recent_trades:
        send_to_kafka(recent_trades, 'stock_prices')

# Run every 60 seconds
if __name__ == "__main__":
    while True:
        produce_data()
        time.sleep(60)  # Wait 60 seconds before fetching new data
