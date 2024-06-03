import requests
import pandas as pd
from pymongo import MongoClient



api_key = 'kC3Sli5A6kY8Rp8zPcCHu83Z1VPSAC93'

#selecciono los tickers
tickers = ['AMD','NFLX', 'GOOG','V','TSLA','NVDA']

# Connect to the MongoDB server
client = MongoClient('mongodb+srv://gonzalezracigmariano:hola1234@cluster0.qiisexw.mongodb.net/')



def persist_data(url,collection):
    response = requests.get(url)
    data = response.json()
    # elej  la colección
    if isinstance(data, list):
    # Si 'data' es una lista, inserto cada elemento en la colección
        for item in data:
        # Verifico si el registro ya existe en la colección
            if collection.find_one(item) is None:
            # Si el registro no existe, lo inserto en la colección
                collection.insert_one(item)
    else:
    # Si 'data' no es una lista, lo inserto directamente en la colección
            if collection.find_one(data) is None:
        # Si el registro no existe, lo inserto en la colección
                collection.insert_one(data)
    return 0    
    
      
# Elijo la bbdd
db = client['financial_data']

#Defino los endpoints que voy a utilizar desde finalcialmodellingprep
endpoints = ['technical_indicator','historical_quote','earnings-surprises','key-metrics', 'key-metrics-ttm','historical-rating']
TA = ['standardDeviation','wma', 'rsi']


# obtengo los datos de cada endpoint para cada ticker
for ticker in tickers:
    for endpoint in endpoints:
        if endpoint != 'key-metrics' and endpoint != 'technical_indicator' and endpoint != 'historical_quote':
            url = f'https://financialmodelingprep.com/api/v3/{endpoint}/{ticker}?apikey={api_key}' 
            collection = db[ticker+'_'+endpoint]
            persist_data(url,collection)   
        elif endpoint == 'key-metrics':
            url = f'https://financialmodelingprep.com/api/v3/{endpoint}/{ticker}?period=annual&apikey={api_key}'
            collection = db[ticker+'_'+endpoint]
            persist_data(url,collection)
        elif endpoint == 'technical_indicator':
            for tar in TA:
                url = f'https://financialmodelingprep.com/api/v3/{endpoint}/1day/{ticker}?type={tar}&period=15&apikey={api_key}'
                collection = db[ticker+'_'+tar]
                persist_data(url,collection)
        else: 
            url = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=wma&period=1&apikey={api_key}'
            collection = db[ticker+'_'+endpoint]
            persist_data(url,collection)





    