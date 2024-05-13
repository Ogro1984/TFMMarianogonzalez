import requests
import pandas as pd
from pymongo import MongoClient

# Define the API key and the tickers

tickers = ['NFLX', 'GOOG', 'AMD']

# Connect to the MongoDB server
client = MongoClient('mongodb+srv://gonzalezracigmariano:hola123@cluster0.qiisexw.mongodb.net/')

# Choose the database
db = client['financial_data']

# Define the endpoints
endpoints = ['quote-short', 'stock-price-change', 'earnings-surprises', 'company-rating', 'key-metrics', 'key-metrics-ttm', 'wma', 'rsi', 'sd', 'historical-price-full']

# Fetch the data for each ticker and endpoint
for ticker in tickers:
    for endpoint in endpoints:
        url = f'https://financialmodelingprep.com/api/v3/{endpoint}/{ticker}'
        response = requests.get(url)
        data = response.json()

        # Choose the collection
        collection = db[endpoint]

        # Insert the data into the collection
        collection.insert_one(data)


    