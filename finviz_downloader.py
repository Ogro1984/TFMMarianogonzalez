import warnings
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd

warnings.filterwarnings('ignore')


sia = SentimentIntensityAnalyzer()

# Obteniendo la URL del artículo
finviz_url = 'https://finviz.com/quote.ashx?t='

# Seleccion de tickers
tickers = ['NFLX', 'GOOG', 'AMD']

news_tables = {}

# Iterar sobre los tickers para obtener los datos HTML
for ticker in tickers:
    url = finviz_url + ticker

    req = Request(url=url, headers={'user-agent':'my-app'})
    response = urlopen(req)

    html = BeautifulSoup(response, 'html')

    news_table = html.find(id='news-table')
    news_tables[ticker] = news_table

parsed_news = []

# Analizar las tablas de noticias
for ticker, news_table in news_tables.items():
    for row in news_table.findAll('tr'):
        title = row.a.text
        date_data = row.td.text.split()

        if len(date_data) == 1:
            time = date_data[0]
        else:
            date = date_data[0]
            time = date_data[1]

        parsed_news.append([ticker, date, time, title, sia.polarity_scores(title)])

# Convertir las noticias analizadas en un DataFrame
df = pd.DataFrame(parsed_news, columns=['ticker', 'date', 'time', 'title', 'sentiment'])

# Conectarse al servidor MongoDB
client = MongoClient('mongodb+srv://gonzalezracigmariano:hola123@cluster0.qiisexw.mongodb.net/')

# Elegir la base de datos y la colección
db = client['finviz']
collection = db['news']

# Convertir el DataFrame en una lista de diccionarios e insertarlo en la colección
collection.insert_many(df.to_dict('records'))