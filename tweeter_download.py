
import tweepy
from textblob import TextBlob
from pymongo import MongoClient
import datetime
import os


# Set the proxy
os.environ['http_proxy'] = 'http://10.83.196.52:80'
os.environ['https_proxy'] = 'https://10.83.196.52:80'


# Twitter API credentials
consumer_key = "DMXPhnTOLE5jCu2hqEIziSjsf"
consumer_secret = "fvkmuhZNwYFYBXaa3XyHCbmTuI9hex8yZRuZw21QdJU405jaou"
access_key = "787107088002535424-DWkbqH6jdODr32ErZHKTxpQohSVc548"
access_secret = "DDpPAxoPHwrApGXWseyZooysj8YU1PUhFAl8P7tQh2ilw"

# Authenticate to Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

# Create API object
api = tweepy.API(auth)

# List of Twitter accounts
accounts = ["JohnLothian", "MarkThoma", "Frances_Coppola"]

# Get the date one week ago
one_week_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)

# Loop through each account
for account in accounts:
    # Get the latest 100 tweets with the hashtag #ticker
    tweets = tweepy.Cursor(api.user_timeline, screen_name=account, tweet_mode='extended').items(10)
    
    filtered_tweets = [tweet for tweet in tweets if tweet.created_at > one_week_ago and '#AAPL' in tweet.full_text]


    # Perform sentiment analysis on each tweet
    sentiment = 0
    if filtered_tweets: 
        for tweet in tweets:
            analysis = TextBlob(tweet.full_text)
            if analysis.sentiment.polarity > 0:
                sentiment = analysis.sentiment.polarity
                print(f"{account} - Positive tweet: {tweet.full_text}\n")

            elif analysis.sentiment.polarity == 0:
                sentiment = analysis.sentiment.polarity
                print(f"{account} - Neutral tweet: {tweet.full_text}\n")
            else:
                sentiment = analysis.sentiment.polarity 
                print(f"{account} - Negative tweet: {tweet.full_text}\n")
    else: 
            sentiment = 0

# Connect to MongoDB
client = MongoClient('mongodb+srv://gonzalezracigmariano:hola123@cluster0.qiisexw.mongodb.net/')
db = client['twitter_db']
collection = db['tweets']
    
# Perform sentiment analysis on each tweet and store in MongoDB
for tweet in tweets:
        analysis = TextBlob(tweet.full_text)
        sentiment = 'neutral'
        if analysis.sentiment.polarity > 0:
            sentiment = 'positive'
        elif analysis.sentiment.polarity < 0:
            sentiment = 'negative'
        
        # Store tweet info in MongoDB
        collection.insert_one({
            'account': account,
            'tweet': tweet.full_text,
            'sentiment': sentiment
        })

