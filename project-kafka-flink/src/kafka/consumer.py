from kafka import KafkaConsumer
import json

# Function to consume messages from the Kafka topic
def consume_messages():
    # Create a Kafka consumer to listen to the 'stock_prices' topic
    consumer = KafkaConsumer(
        'stock_prices',               # Topic to listen to
        bootstrap_servers='localhost:9092',  # Kafka server
        auto_offset_reset='earliest',  # Read messages from the beginning of the topic
        group_id='stock-price-consumer'  # Consumer group ID
    )
    
    print("Waiting for new messages on the 'stock_prices' topic...")
    
    # Consume messages
    for message in consumer:
        # Decode the message from JSON format
        stock_data = json.loads(message.value.decode('utf-8'))
        print(f"Message received: {stock_data}")


if __name__ == "__main__":
    consume_messages()
