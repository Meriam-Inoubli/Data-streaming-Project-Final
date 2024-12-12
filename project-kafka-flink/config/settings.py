import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# API Configuration
API_SECRET = os.getenv("API_SECRET")
API_KEY = os.getenv("API_KEY")

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Flink Configuration
FLINK_HOST = os.getenv("FLINK_HOST")
