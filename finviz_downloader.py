import warnings
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')


sia = SentimentIntensityAnalyzer()

# Obteniendo la URL del artículo
finviz_url = 'https://finviz.com/quote.ashx?t='
#finviz_url = 'services.cnn.com'
# Seleccion de tickers
tickers = ['AMD','NFLX', 'GOOG','V','TSLA','NVDA']

news_tables = {}

client = MongoClient('mongodb+srv://gonzalezracigmariano:hola1234@cluster0.qiisexw.mongodb.net/')

# Elegir la base de datos y la colección
db = client['finviz2']
collection = db['news']
# Iterar sobre los tickers para obtener los datos HTML
for ticker in tickers:
    url = finviz_url + ticker

    req = Request(url=url, headers={'user-agent':'my-app'})
    response = urlopen(req)

    html = BeautifulSoup(response, 'html')

    news_table = html.find(id='news-table')
    news_tables[ticker] = news_table

 # Iterar sobre las filas de la tabla de noticias
    for row in news_table.findAll('tr'):
        title = row.a.text
        link = row.a['href']
        date_data = row.td.text.split()

        # Si la longitud de date_data es 1, significa que la fecha y la hora están en diferentes filas
        if len(date_data) == 1:
            date = date_data[0]
        else:
            date = date_data[0]
            time = date_data[1]

        # Crear el artículo
        article = {'ticker': ticker, 'title': title, 'link': link, 'date': date, 'time': time,'polarity':sia.polarity_scores(title).get('compound')}

        # Actualizar el documento en MongoDB si existe, si no existe, lo inserta
        collection.update_one({'link': article['link']}, {'$set': article}, upsert=True)
