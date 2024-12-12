import os
import json
from binance.client import Client
import time

# Load API and Secret keys from environment variables
API_KEY = os.getenv('API_KEY') 
API_SECRET = os.getenv('API_SECRET')  

# Initialize the Binance client with API keys
client = Client(API_KEY, API_SECRET)

# Function to fetch OHLCV data (price history)
def get_ohlcv(symbol, interval='1h'):
    # Use the API to fetch OHLCV data
    klines = client.get_klines(symbol=symbol, interval=interval)
    if klines:
        return klines  # List of OHLCV data
    else:
        print("Error while fetching OHLCV data")
        return None

# Function to fetch the order book
def get_order_book(symbol):
    order_book = client.get_order_book(symbol=symbol)
    if order_book:
        return order_book 
    else:
        print("Error while fetching the order book")
        return None

# Function to fetch recent trades
def get_recent_trades(symbol):
    recent_trades = client.get_recent_trades(symbol=symbol)
    if recent_trades:
        return recent_trades  # List of recent trades
    else:
        print("Error while fetching recent trades")
        return None

# Main function to fetch data without saving it to files
def fetch_data():
    symbol = "BTCUSDT"
    
    ohlcv_data = get_ohlcv(symbol)
    order_book = get_order_book(symbol)
    recent_trades = get_recent_trades(symbol)
    
    return ohlcv_data, order_book, recent_trades

# Execute every 60 seconds
if __name__ == "__main__":
    while True:
        fetch_data()
        time.sleep(60)  # Wait 60 seconds before fetching new data
