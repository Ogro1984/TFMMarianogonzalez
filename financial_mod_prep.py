import requests
import pandas as pd
from pymongo import MongoClient


#selecciono los tickers
tickers = ['NFLX', 'GOOG', 'AMD']

# Connect to the MongoDB server
client = MongoClient('mongodb+srv://gonzalezracigmariano:hola123@cluster0.qiisexw.mongodb.net/')

# Elijo la bbdd
db = client['financial_data']

#Defino los endpoints que voy a utilizar desde finalcialmodellingprep
endpoints = ['quote-short', 'stock-price-change', 'earnings-surprises', 'company-rating', 'key-metrics', 'key-metrics-ttm', 'wma', 'rsi', 'sd', 'historical-price-full']

# obtengo los datos de cada endpoint para cada ticker
for ticker in tickers:
    for endpoint in endpoints:
        url = f'https://financialmodelingprep.com/api/v3/{endpoint}/{ticker}'
        response = requests.get(url)
        data = response.json()

        # elej  la colección
        collection = db[endpoint]

        # inserto los datos en la colección
        collection.insert_one(data)


    