import pandas as pd
from pymongo import MongoClient
from keras.models import Sequential
from keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
import numpy as np

# Connect to the MongoDB server
client = MongoClient('mongodb+srv://gonzalezracigmariano:hola123@cluster0.qiisexw.mongodb.net/')

# seleccionar la base de datos y la colección
db = client['finviz']
collection = db['news']

db = client['financial_data']

# obtener los datos de la colección
data = pd.DataFrame(list(collection.find()))

# filtrar los datos
data['date'] = pd.to_datetime(data['date'])
data = data.set_index('date')
data = data.loc['2022-01-01':'2024-12-31']

# partir los datos en entrenamiento y prueba
train = data.loc['2022-01-01':'2023-12-31']
test = data.loc['2024-01-01':'2024-12-31']

# normalizar los datos
scaler = MinMaxScaler(feature_range=(0, 1))
train = scaler.fit_transform(train)
test = scaler.transform(test)

# reformatear los datos a la forma [samples, time steps, features] 3D
train = np.reshape(train, (train.shape[0], 1, train.shape[1]))
test = np.reshape(test, (test.shape[0], 1, test.shape[1]))

# construir el modelo LSTM
model = Sequential()
model.add(LSTM(50, input_shape=(train.shape[1], train.shape[2])))
model.add(Dense(1))
model.compile(loss='mae', optimizer='adam')


model.fit(train, epochs=50, batch_size=72, validation_data=(test), verbose=2, shuffle=False)

#probando el modelo
predictions = model.predict(test)

predictions = scaler.inverse_transform(predictions)

rmse = np.sqrt(mean_squared_error(test, predictions))
# print('Test RMSE: %.3f' % rmse)