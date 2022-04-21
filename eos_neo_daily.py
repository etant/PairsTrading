from key import *
from binance.client import Client
import pandas as pd
import time
from datetime import datetime, tzinfo, timedelta
import talib
from talib.abstract import *
from statsmodels.tsa.stattools import adfuller
import statsmodels.api as sm
import numpy as np
import pytz
#pip install TA-Lib

def getBinanceDataFuture(symbol, interval, start, end):
    df = pd.DataFrame()
    startDate = end
    prev = start
    #while loop to get all the data because binance api limits each call
    while (startDate!=prev):
        prev = startDate
        url = 'https://fapi.binance.com/fapi/v1/klines?symbol=' + symbol + '&interval=' + interval + '&endTime=' + str(startDate)
        df2 = pd.read_json(url)
        df2.columns = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Closetime', 'Quote asset volume', 'Number of trades','Taker by base', 'Taker buy quote', 'Ignore']
        df = pd.concat([df2, df], axis=0, ignore_index=True, keys=None)
        startDate = df.Opentime[0]
    #format and drop duplicate data
    df.reset_index(drop=True, inplace=True)
    df['Opentime'] = pd.to_datetime(df['Opentime'],unit='ms')
    df = df.loc[1:]
    df = df[['Opentime', 'Open', 'High', 'Low', 'Close']]
    df = df.set_index('Opentime')
    df = df.drop_duplicates(keep='first')
    return df



def signalf(varaible,a,b,a_name,b_name):
    lookback,sdenter,sdexit,sdloss = varaible
    lookback = int(lookback)


    #merge data
    pairPrice = a.merge(b, left_index=True, right_index=True, how='left')
    pairPrice = pairPrice.dropna()

    reg = sm.OLS(pairPrice['Close_x'],pairPrice['Close_y']).fit()
    reg2 = sm.OLS(pairPrice['Close_y'],pairPrice['Close_x']).fit()

    #see which token should be a the dependent variable
    if adfuller(reg.resid)[1]<adfuller(reg2.resid)[1]:
        y = pairPrice['Close_x']
        x = pairPrice['Close_y']

        hedge = reg.params[0]
        spread = reg.resid
        y_name = a_name
        x_name = b_name

    else:
        y = pairPrice['Close_y']
        x = pairPrice['Close_x']

        hedge = reg2.params[0]
        spread = reg2.resid
        y_name = a_name
        x_name = b_name



    #creating enter,exit, stop loss boilerger bands
    bands = pd.DataFrame(BBANDS(spread,timeperiod=lookback,nbdevup=sdenter,nbdevdn = sdenter,matype=1)).T
    bands = bands.set_index(pairPrice.index)
    bands.columns = ['Short','1mid','Long']

    bands2 = pd.DataFrame(BBANDS(spread,timeperiod=lookback,nbdevup=sdexit,nbdevdn = sdexit,matype=1)).T
    bands2 = bands2.set_index(pairPrice.index)
    bands2.columns = ['exShort','mid','exLong']

    bands3 = pd.DataFrame(BBANDS(spread,timeperiod=lookback,nbdevup=sdloss,nbdevdn = sdloss,matype=1)).T
    bands3 = bands3.set_index(pairPrice.index)
    bands3.columns = ['stShort','mid2','stLong']
    bbands = bands2.join(bands)
    bbands = bbands.join(bands3)
    bbands = bbands.drop(columns = ['mid','1mid','mid2'])

    bbands['y'] = y
    bbands['x'] = x
    bbands['spread'] = spread
    bbands['hedge'] =hedge


    bbands = bbands.dropna()


    return bbands,y_name,x_name

#binance has different quantity precision for every token
def quantityPercision(symbol,size):
    info = client.futures_exchange_info()
    for x in info['symbols']:
        if x['symbol'] == symbol:
            prec =  x['quantityPrecision']
    factor = 10.0**prec
    return int(size*factor)/factor

#get time right now in miliseconds
def current_milli_time():
    return round(time.time() * 1000)

#-1 is short spread, 0 is stay the same, while 1 is long spread
def getPostion(signal,currentPos):
    #if current position is nothing
    if currentPos ==[0,0]:
        #if spread is between short and stoploss short signal, short
        if (signal["spread"]>signal["Short"])&(signal["spread"]<signal["stShort"]):
            return -1
        #if spread is between long and stop loss long signal, long
        elif (signal["spread"]<signal["Long"]) & (signal["spread"]>signal["stLong"]):
            return 1
        #if spread is not in range of signals, do nothing
        else:
            return 0
    #if current position is short spread
    elif (np.sign(currentPos)==[-1,1]).all():
        #if spread, hit below the exit threshold, or went above stop loss threshold, either exit or go long
        if (signal["spread"]<signal["exShort"])|(signal["spread"]>signal["stShort"]):
            # if spread goes into long territory, enter long
            if (signal["spread"]<signal["Long"])&(signal["spread"]>signal["stLong"]):
                return 1
            #if spread is not in long territory but triggered exit, exit
            else:
                return 0
        #else stay short
        else:
            return -1
    #if current postion is long spread
    else:
        #if spread hit the stop loss or exit signal, either exit or go short
        if (signal["spread"]>signal["exLong"])|(signal["spread"]<signal["stLong"]):
            # if spread goes into short territory, enter short
            if (signal["spread"]>signal["Short"])&(signal["spread"]<signal["stShort"]):
                return -1
            #spread not in short terrirtory, so exit
            else:
                return 0
        #else stay long
        else:
            return 1


if __name__ == '__main__':
    # create binance api connection
    client = Client(api_key, api_secret)
    #creat a trading log to record all transactions, make sure what is signaled is being excuted
    log = open("tradingLog.txt", "a")
    #token being traded
    token1="EOSUSDT"
    token2="NEOUSDT"
    #leverage for the trades
    new_lvrg = 1
    client.futures_change_leverage(symbol = token1,leverage=new_lvrg)
    client.futures_change_leverage(symbol = token2,leverage=new_lvrg)
    # a never ending while loop, that trades every day at UTC midnight
    while True:
        #find out how many seconds to sleep to, untill UTC midnight
        dt = datetime.utcnow().date()
        today = datetime.combine(dt, datetime.min.time())
        tmr = today + timedelta(days = 1)
        now = datetime.utcnow()
        sleeptime = (tmr-now).seconds
        time.sleep(sleeptime)
        #write down todays date on the trading log
        utc_now_dt = datetime.now(tz=pytz.UTC)
        log.write(str(utc_now_dt.strftime("%d/%m/%Y %H:%M:%S"))+": "+"\n")
        #get price data of tokens
        token1data = getBinanceDataFuture(token1,'1d',1458955882,current_milli_time())
        token2data = getBinanceDataFuture(token2,'1d',1458955882,current_milli_time())
        #LookBack Period, SD enter, Sd exit, stoploss
        param = [10, 1.8, 0.8, 4.0]
        bbands,y_name,x_name = signalf(param,token1data,token2data,token1,token2)
        #get todays signal
        signal = bbands.iloc[-1]
        #get current account situation, cap is times by .99 to include slipage and fees
        cap = float(client.futures_account()["totalMarginBalance"])*.99
        df = pd.DataFrame(client.futures_account()['positions'])
        df = df.apply(lambda col:pd.to_numeric(col, errors='ignore'))
        #get current long short position size
        currentPos = [float(df[df["symbol"]==y_name].positionAmt),float(df[df["symbol"]==x_name].positionAmt)]
        position = getPostion(signal,currentPos)
        #if signal is long or short
        if position !=0:
            #if signal went from short to long or long to short, exit current positions
            if (position != np.sign(currentPos[0])) & (currentPos[0]!=0):
                try:
                    currentLS = ['SELL' if i < 0 else 'BUY' for i in currentPos]
                    client.futures_create_order(symbol=x_name,side=currentLS[0],type="MARKET",reduceOnly = True,quantity = abs(currentPos[1]))
                    client.futures_create_order(symbol=y_name,side=currentLS[1],type="MARKET",reduceOnly = True,quantity =abs(currentPos[0]))
                    currentPos=[0,0]
                except Exception as e:
                        log.write("There was an error: " + str(e)+"\n")
            #if current position isn't long or short, then execute long or shor trade
            if currentPos==[0,0]:
                #get percetange of y and x
                yp = signal["y"]/(signal["x"]*signal["hedge"]+signal["y"])
                xp = 1-yp
                #get sizing
                size = np.array([position*cap*yp/float(client.futures_symbol_ticker(symbol = y_name)['price']),-1*position*cap*xp/float(client.futures_symbol_ticker(symbol = x_name)['price'])])
                #if its negative size, then sell, else its buy
                LS = ['SELL' if i < 0 else 'BUY' for i in size]
                try:
                    #execute orders
                    client.futures_create_order(symbol=y_name,type='MARKET',side=LS[0],quantity=quantityPercision(y_name,abs(size[0])))
                    client.futures_create_order(symbol=x_name,type='MARKET',side=LS[1],quantity=quantityPercision(x_name,abs(size[1])))
                    # get executed price and quantity
                    df = pd.DataFrame(client.futures_account()['positions'])
                    df = df.apply(lambda col:pd.to_numeric(col, errors='ignore'))
                    df = df[df["positionAmt"]!=0]
                    log.write("executed: "+"\n")
                    for i in df.index:
                        log.write(df["symbol"][i] + " price: " +str(float(df["entryPrice"][i] )) + " quant: " + str(float(df["positionAmt"][i]))+"\n")
                    #get execpected quanitty and price, compare to see if there is a difference
                    log.write("expectations: "+"\n")
                    log.write(y_name + " price: " +str(signal.y) + " quant: " + str(size[0])+"\n")
                    log.write(x_name + " price: " +str(signal.x) + " quant: " + str(size[1])+"\n")
                except Exception as e:
                        log.write("There was an error: " + str(e)+"\n")
            # if current signal is same as current position, do nothing
            else:
                pass
        #if signal is to exit position
        else:
            #if signal is to exit postion, while no current position, do nothing
            if currentPos==[0,0]:
                pass
            # if signal is to exit postion, and current position is long or short, exit postion
            else:
                try:
                    currentLS = ['SELL' if i < 0 else 'BUY' for i in currentPos]
                    client.futures_create_order(symbol=x_name,side=currentLS[0],type="MARKET",reduceOnly = True,quantity = abs(currentPos[1]))
                    client.futures_create_order(symbol=y_name,side=currentLS[1],type="MARKET",reduceOnly = True,quantity =abs(currentPos[0]))
                    log.write("expectations: "+"\n")
                    log.write(y_name + " price: " +str(signal.y)+"\n")
                    log.write(x_name + " price: " +str(signal.x)+"\n")
                except Exception as e:
                        log.write("There was an error: " + str(e)+"\n")
        log.write("Balance: "+str(client.futures_account()['totalMarginBalance'])+"\n")
        log.write("----------------------------------"+"\n")
        log.flush()
