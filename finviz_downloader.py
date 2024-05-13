import warnings
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd

warnings.filterwarnings('ignore')

# Initialize the sentiment analyzer
sia = SentimentIntensityAnalyzer()

# Fetching the article url
finviz_url = 'https://finviz.com/quote.ashx?t='

# Choosing our tickers
tickers = ['NFLX', 'GOOG', 'AMD']

news_tables = {}

# Iterating over tickers to get the html data
for ticker in tickers:
    url = finviz_url + ticker

    req = Request(url=url, headers={'user-agent':'my-app'})
    response = urlopen(req)

    html = BeautifulSoup(response, 'html')

    news_table = html.find(id='news-table')
    news_tables[ticker] = news_table

parsed_news = []

# Parse the news tables
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

# Convert the parsed news into a DataFrame
df = pd.DataFrame(parsed_news, columns=['ticker', 'date', 'time', 'title', 'sentiment'])

# Connect to the MongoDB server
client = MongoClient('mongodb+srv://gonzalezracigmariano:hola123@cluster0.qiisexw.mongodb.net/')

# Choose the database and collection
db = client['finviz']
collection = db['news']

# Convert the DataFrame into a list of dictionaries and insert it into the collection
collection.insert_many(df.to_dict('records'))
