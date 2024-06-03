# Imports generales
import pandas as pd
import io
from google.colab import files
import numpy as np
import time
from pymongo import MongoClient
tickers = ['AMD','NFLX', 'GOOG','V','TSLA','NVDA']



client = MongoClient('mongodb+srv://gonzalezracigmariano:hola1234@cluster0.qiisexw.mongodb.net/')

# Elegir la base de datos y la colección
db = client['financial_data']
collection = db['AMD_historical-rating']


# Fetch all the documents from the collection
documents = collection.find()

df = pd.DataFrame(list(documents))

#df.head(100)
# # Select only the columns you're interested in
df = df[['date', 'symbol', 'ratingScore']]
df['date'] = pd.to_datetime(df['date']) 
df = df.rename(columns={'symbol': 'ticker'})

collection = db['AMD_historical_quote']

# Fetch all the documents from the collection
documents = collection.find()

# Convert the documents into a DataFrame
df1 = pd.DataFrame(list(documents))

# Select only the columns you're interested in and add a new column 'ticker' with value 'AMD'
df1 = df1[['date', 'close','volume']]
df1['ticker'] = 'AMD'
# Convert 'date' to datetime format and truncate the hour


df1['date'] = pd.to_datetime(df['date']).dt.date

df1['date'] = pd.to_datetime(df1['date'])
df['date'] = pd.to_datetime(df['date'])
# Assuming 'df1' and 'df2' are your DataFrames
df = pd.merge(df, df1[['date', 'close','volume']], on='date', how='left')

collection = db['AMD_rsi']
# Fetch all the documents from the collection
documents = collection.find()
# Convert the documents into a DataFrame
df2 = pd.DataFrame(list(documents))

# Select only the columns you're interested in and add a new column 'ticker' with value 'AMD'
df2 = df2[['date', 'rsi']]
df2['ticker'] = 'AMD'
# Convert 'date' to datetime format and truncate the hour


df2['date'] = pd.to_datetime(df2['date']).dt.date

df2['date'] = pd.to_datetime(df2['date'])

# Assuming 'df1' and 'df2' are your DataFrames
df = pd.merge(df, df2[['date', 'rsi']], on='date', how='left')


collection = db['AMD_standardDeviation']
# Fetch all the documents from the collection
documents = collection.find()
# Convert the documents into a DataFrame
df3 = pd.DataFrame(list(documents))

# Select only the columns you're interested in and add a new column 'ticker' with value 'AMD'
df3 = df3[['date', 'standardDeviation']]
df3['ticker'] = 'AMD'
# Convert 'date' to datetime format and truncate the hour


df3['date'] = pd.to_datetime(df3['date']).dt.date

df3['date'] = pd.to_datetime(df3['date'])

# Assuming 'df1' and 'df2' are your DataFrames
df = pd.merge(df, df3[['date', 'standardDeviation']], on='date', how='left')

collection = db['AMD_wma']
# Fetch all the documents from the collection
documents = collection.find()
# Convert the documents into a DataFrame
df4 = pd.DataFrame(list(documents))

# Select only the columns you're interested in and add a new column 'ticker' with value 'AMD'
df4 = df4[['date', 'wma']]
df4['ticker'] = 'AMD'
# Convert 'date' to datetime format and truncate the hour


df4['date'] = pd.to_datetime(df4['date']).dt.date

df4['date'] = pd.to_datetime(df4['date'])

# Assuming 'df1' and 'df2' are your DataFrames
df = pd.merge(df, df4[['date', 'wma']], on='date', how='left')

df.drop("date", axis = 1, inplace = True)

df.drop("date", axis = 1, inplace = True)

#divido los set de entrenamiento y testiong
# Assuming 'df' is your DataFrame

test = df.head(30)
train = df.iloc[30:]

train = train.interpolate()
test=test.interpolate()
neighbors=15
# constructor
n_neighbors = 15
weights = 'uniform'
knn = neighbors.KNeighborsRegressor(n_neighbors= n_neighbors, weights=weights)
# fit and predict
#auqui como no tenemos las etiquetas de test para validar el error de nuestro modelo nos limitaremos a guardar las predicciones en una variable de salida
knn.fit( train.drop(['close'], axis=1), y = train['close'])

y_pred= knn.predict(test.drop('close',axis=1))

prediction_df = pd.DataFrame(y_pred, columns=['Prediction'])

prediction_df = pd.concat([df.head(30).reset_index(drop=True), prediction_df], axis=1)

db = client['predicciones']
collection = db['amd_predicciones']


records = prediction_df.to_dict('records')

# For each record
for record in records:
    # Define a query that will find the document if it exists
    query = {'Prediction': record['Prediction']}
    
    # Define an update that will replace the document
    update = {"$set": record}
    
    # Use upsert=True to insert the document if it doesn't exist
    collection.update_one(query, update, upsert=True)