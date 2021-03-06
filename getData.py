import pandas as pd
from datetime import datetime
import time
from itertools import combinations
import numpy as np
import math
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller


def getBinanceDataFuture(symbol, interval, start, end, limit=5000):
    df = pd.DataFrame()
    startDate = end
    prev = start
    while (startDate!=prev):
        prev = startDate
        url = 'https://fapi.binance.com/fapi/v1/klines?symbol=' + \
            symbol + '&interval=' + interval + '&endTime=' + str(startDate)

        df2 = pd.read_json(url)
        df2.columns = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Closetime', 'Quote asset volume', 'Number of trades','Taker by base', 'Taker buy quote', 'Ignore']
        df = pd.concat([df2, df], axis=0, ignore_index=True, keys=None)
        startDate = df.Opentime[0]

    df.reset_index(drop=True, inplace=True)
    df['Opentime'] = pd.to_datetime(df['Opentime'],unit='ms')
    df = df.loc[1:]
    df = df[['Opentime', 'Open', 'High', 'Low', 'Close']]
    df = df.set_index('Opentime')
    df = df.drop_duplicates(keep='first')
    return df

def getBinanceDataFundingRate(symbol, interval, start, end, limit=1000):
    df = pd.DataFrame()
    startDate = start
    prev = 0
    while (startDate!=prev):
        prev = startDate
        url = 'https://fapi.binance.com/fapi/v1/fundingRate?symbol=' + \
            symbol + '&startTime=' + str(startDate) + '&limit=' + str(limit)

        df2 = pd.read_json(url)
        df2.columns = ['symbol','fundingTime','fundingRate']

        df = pd.concat([df, df2], axis=0, ignore_index=True, keys=None)
        startDate = df2.fundingTime[len(df2)-1]

    df.reset_index(drop=True, inplace=True)
    df['fundingTime'] = pd.to_datetime(df['fundingTime'],unit='ms')
    #df = df.loc[1:]
    #df = df.set_index('fundingTime')
    df = df.drop_duplicates(keep='first')
    df['fundingTime'] = df['fundingTime'].dt.floor('Min')
    df = df.set_index('fundingTime')
    return df

def dataOrganizeDaily(ticker):
    fr =  getBinanceDataFundingRate(ticker,'1d',1458955882,current_milli_time())
    data = getBinanceDataFuture(ticker,'1d',1458955882,current_milli_time())
    date = fr.index.get_level_values('fundingTime').floor('D')
    dailyfr = fr.groupby([date]).sum().reset_index()
    dailyfr = dailyfr.set_index('fundingTime')
    data['fr'] = dailyfr['fundingRate']
    return data

def dataOrganizeHourly(ticker):
    fr =  getBinanceDataFundingRate(ticker,'1h',1458955882,current_milli_time())
    price = getBinanceDataFuture(ticker,'1h',1458955882,current_milli_time())
    data = convertFRhourly(fr,price)
    return data

def convertFRhourly(fr,price):
    fr["fundingRate"] = fr["fundingRate"]/8
    fr = fr.reindex(pd.date_range(price.index[0],price.index[-1],freq='1h'))
    fr = fr.ffill()
    price["fr"] = fr["fundingRate"]
    return price

def current_milli_time():
    return round(time.time() * 1000)

if __name__ == '__main__':

    token1 = input("Enter first token (all caps plus USDT such as BTCUSDT): ")
    token2 = input("Enter another:")
    hOrD = input("Hourly or Daily (H/D):")
    if hOrD == "H":
        token1Data = dataOrganizeHourly(token1)
        token2Data = dataOrganizeHourly(token2)
    else:
        token1Data = dataOrganizeDaily(token1)
        token2Data = dataOrganizeDaily(token2)

    merged = token1Data.merge(token2Data, left_index=True, right_index=True, how='left')
    merged = merged.dropna()

    reg = sm.OLS(merged['Open_x'],merged['Open_y']).fit()
    reg2 = sm.OLS(merged['Open_y'],merged['Open_x']).fit()

#     #see which token should be a the dependent variable
    if adfuller(reg.resid)[1]<adfuller(reg2.resid)[1]:
        y = token1Data[['Open','fr']]
        x = token2Data[['Open','fr']]

    else:
        x = token2Data[['Open','fr']]
        y = token1Data[['Open','fr']]

    end = x.merge(y, left_index=True, right_index=True, how='left').dropna()
    end.to_csv("data.csv");
