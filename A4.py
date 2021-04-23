# Author: Saman Kashanchi
# StudentId = 2301023
# Assignment 4 - Data generation/import


import mysql.connector
from faker import Faker
import pandas as pd
import datetime


db =  mysql.connector.connect(host = "34.94.182.22", user = "kashanchi@chapman.edu", passwd ="FooBar!@#$", database = "kashanchi_db")
fake = Faker()


#Creating data Frame For Faker data storage
Pricedf = pd.DataFrame()
CloseList = []
OpenList = []
HighList = []
LowList = []
VolumeList = []
#Creating Faker data in the appropriate ranges for varibals
for i in range(100):
    OpenList.append(fake.random_int(min=0, max=10, step=1))
    HighList.append(fake.random_int(min=5, max=10, step=1))
    LowList.append(fake.random_int(min=0, max=5, step=1))
    CloseList.append(fake.random_int(min=0, max=5, step=1))
    VolumeList.append(fake.random_int(min=100, max=1000, step=50))
#Creating dates list for PK
date_list = [d.strftime('%Y%m%d') for d in pd.date_range('20130226','20130605')]

#Placing data in correct col in Df
Pricedf["Date"] = date_list
Pricedf["Open"] = OpenList
Pricedf["High"] = HighList
Pricedf["Low"] = LowList
Pricedf["Close"] = CloseList
Pricedf['Volume'] = VolumeList

## Repeating the same process for data generation and storage for other tables

Meansdf = pd.DataFrame()
Means = []
MeansReturns = []
for i in range(20):
    Means.append(fake.random_int(min=0, max=10, step=1))
    MeansReturns.append(fake.random_int(min=-100, max=100, step=2))

Meansdf["Date"] = date_list[::5]
Meansdf["Means"] = Means
Meansdf["%Returns"] = MeansReturns




Statsdf = pd.DataFrame()
variance = []
RSI = []
MACD = []
for i in range(100):
    variance.append(fake.random_int(min=0, max=3, step=1))
    RSI.append(fake.random_int(min=-5, max=5, step=1))
    MACD.append(fake.random_int(min=0, max=100, step=2))

Statsdf["Date"] = date_list
Statsdf["variance"] = variance
Statsdf["RSI"] = RSI
Statsdf["MACD"] = MACD


Chartsdf = pd.DataFrame()
Support = []
Resistance = []
ADX = []
Fibonacci =[]
for i in range(100):
    Support.append(fake.random_int(min=0, max=3, step=1))
    Resistance.append(fake.random_int(min=0, max=3, step=1))
    ADX.append(fake.random_int(min=0, max=100, step=2))
    Fibonacci.append(fake.random_int(min=0, max=100, step=2))

Chartsdf["Date"] = date_list
Chartsdf["Support"] = Support
Chartsdf["Resistance"] = Resistance
Chartsdf["ADX"] = ADX
Chartsdf["Fibonacci"] = Fibonacci



MeanRevdf = pd.DataFrame()
Gains = []
Loss = []
DrawDown = []
for i in range(100):
    Gains.append(fake.random_int(min=0, max=3, step=1))
    Loss.append(fake.random_int(min=0, max=3, step=1))
    DrawDown.append(fake.random_int(min=0, max=100, step=2))

MeanRevdf["Date"] = date_list
MeanRevdf["Gains"] = Gains
MeanRevdf["Loss"] = Loss
MeanRevdf["DrawDown"] = DrawDown


#Creating CSV outputs for each table
Pricedf.to_csv("PriceDataFrame.csv" ,index=False, header=["DateTime","open", "high", "low", "close","volume"])
Meansdf.to_csv("MeansDataFrame.csv" ,index=False, header=["DateTime","Means", "%Returns"])
Statsdf.to_csv("StatsDataFrame.csv" ,index=False, header=["DateTime","variance", "RSI", "MACD"])
Chartsdf.to_csv("ChartsDataFrame.csv" ,index=False, header=["DateTime","Support", "Resistance", "ADX","Fibonacci"])
MeanRevdf.to_csv("MeansStratDataFrame.csv" ,index=False, header=["DateTime","Gains", "Loss", "DrawDown"])



mycursor = db.cursor()
#Inserting form our created dfs into our sql table

for i, row in Pricedf.iterrows():
    #checking if table is empty
    mycursor.execute(" select exists(select 1 from PriceTable) AS Output;")
    data = pd.DataFrame(mycursor.fetchall())
    answer = data[0][0]
    #inserting the data in
    if answer != 1:
        sql = "INSERT INTO PriceTable VALUES (%s,%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple(row))
        print("Record inserted")
    else:
        break
    db.commit()


for i, row in Meansdf.iterrows():
    mycursor.execute(" select exists(select 1 from 5Means) AS Output;")
    data = pd.DataFrame(mycursor.fetchall())
    answer = data[0][0]
    if answer != 1:
        sql = "INSERT INTO 5Means VALUES (%s,%s,%s)"
        mycursor.execute(sql, tuple(row))
        print("Record inserted")
    else:
        break
    db.commit()


for i, row in Statsdf.iterrows():
    mycursor.execute(" select exists(select 1 from Statistics) AS Output;")
    data = pd.DataFrame(mycursor.fetchall())
    answer = data[0][0]
    if answer != 1:
        sql = "INSERT INTO Statistics VALUES (%s,%s,%s,%s)"
        mycursor.execute(sql, tuple(row))
        print("Record inserted")
    else:
        break
    db.commit()

for i, row in Chartsdf.iterrows():
    mycursor.execute(" select exists(select 1 from ChartAnalysis) AS Output;")
    data = pd.DataFrame(mycursor.fetchall())
    answer = data[0][0]
    if answer != 1:
        sql = "INSERT INTO ChartAnalysis VALUES (%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple(row))
        print("Record inserted")
    else:
        break
    db.commit()


for i, row in MeanRevdf.iterrows():
    mycursor.execute(" select exists(select 1 from MeanStrat) AS Output;")
    data = pd.DataFrame(mycursor.fetchall())
    answer = data[0][0]
    if answer != 1:
        sql = "INSERT INTO MeanStrat VALUES (%s,%s,%s,%s)"
        mycursor.execute(sql, tuple(row))
        print("Record inserted")
    else:
        break
    db.commit()


