import numpy as np
import time as tm
import datetime as dt
import tensorflow as tf
import pandas as pd
import yfinance as yf3
from yahoo_fin import stock_info as yf
from yahoofinancials import YahooFinancials as yf2
import math
from datetime import datetime
import os 


targetdf = pd.DataFrame()
targetdf = pd.DataFrame(columns=['Date', 'Ticker', 'Delta','Est','Rep','Suprise'])


def save_sp500_tickers():
    import requests
    import bs4 as bs
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    i=0
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker.strip())
        i=i+1
        #if i > 10:
        #  break
    return tickers

#def writefile(input):
  #print(input)
  #file = open('/Users/tarikessawi/earnings.csv', 'a')
  #file.writelines(input)
  #file.close

def get_dates_ahead(earndate,inputdays):
  date_days_ahead = (datetime.strptime(earndate, "%Y-%m-%d") + dt.timedelta(days=inputdays)).strftime('%Y-%m-%d')
  #print("ahead is "+ date_days_ahead)

  if datetime.strptime(date_days_ahead, "%Y-%m-%d")  > datetime.now():
    #print ("date is newer")
    date_days_ahead = datetime.now()
  return date_days_ahead

def get_dates_back(earndate, inputdays):
  if datetime.strptime(earndate, "%Y-%m-%d") < datetime.now():
    date_days_back = (datetime.strptime(earndate, "%Y-%m-%d") - dt.timedelta(days=inputdays)).strftime('%Y-%m-%d')
  else:
    date_days_back = (datetime.now() - dt.timedelta(days=inputdays)).strftime('%Y-%m-%d')
  return date_days_back
def get_delta_days(d1, d2):
    d1 = datetime.strftime(d1, "%Y-%m-%d")
    d2 = datetime.strftime(d2, "%Y-%m-%d")
    #print(d1)
    #print(d2)
    
    date_days = (datetime.strptime(d2, "%Y-%m-%d") - datetime.strptime(d1, "%Y-%m-%d"))
    #date_days = d2 - d1
    return date_days
 
def process_stockprice(stockdf,earndate):
  #print("here")
  file = open('/Users/tarikessawi/data/stockprices.csv', 'a')  
  for index, pricerow in stockdf.iterrows():
    delta_from_earn = get_delta_days(earndate,index)
    file.write(f"{index.strftime('%Y-%m-%d')},{pricerow['ticker']},{pricerow['close']},{pricerow['adjclose'] },{delta_from_earn.days} \n")

    #print(delta_from_earn)        
    #writeline = index,ticker, pricerow['close'], pricerow['close']
   
def get_targets(stock,days_forward):
    import csv
    import math
    from datetime import datetime
    
    global targetdf
    df3 = pd.DataFrame()
    
    str_d1 = dt.date.today().strftime('%Y-%m-%d')
    try: 
        tick = yf3.Ticker(stock)
        df3 = tick.earnings_dates
    except:
        pass
        print("Exception Occurred")
    i = 0
    #print(df3)
     # Create an empty DataFrame
    
    #file3 = open('/Users/tarikessawi/data/targets.csv', 'a')  
    
    date_now = tm.strftime('%Y-%m-%d')
    for index, earnrow in df3.iterrows():
      str_d2 = index.strftime('%Y-%m-%d')
      delta = datetime.strptime(str_d2, "%Y-%m-%d") - datetime.strptime(str_d1, "%Y-%m-%d")
      #earndelta = get_delta_days(datetime.strptime(str_d2, "%Y-%m-%d"),)
      IsEstimateNan = math.isnan(earnrow['EPS Estimate'])
      earndate = index.strftime('%Y-%m-%d')
      if (IsEstimateNan == False and delta.days > 0 and delta.days < 30):
        new_row = {'Ticker':stock, 'Date':str_d2, 'Delta':delta.days, 'Est':earnrow['EPS Estimate'],'Rep':earnrow['Reported EPS'],'Surprise':earnrow['Surprise(%)']}
       
        targetdf.loc[len(targetdf)] = new_row

        #targetdf = df.append(new_row, ignore_index=True)
        
        #pd.DataFrame([{'Ticker':stock, 'Date':e, 'Delta':delta.days, 'Est':earnrow['EPS Estimate'],'Rep':earnrow['Reported EPS'],'Surprise':earnrow['Surprise(%)']}, columns=['Ticker', 'Date', 'Delta','Est','Rep','Suprise'])
        #df = pd.DataFrame(columns=['Ticker', 'Date', 'Delta','Est','Rep','Suprise'])

        #print(f"{writeline}\n")
        #file3.write(f"{stock},{str_d2},{delta.days},{earnrow['EPS Estimate']},{earnrow['Reported EPS']},{earnrow['Surprise(%)']}\n")
        #targetdf = pd.concat([targetdf,earnrow])
    
    
def get_next_earnings_date(stock):
    import csv
    import math
    from datetime import datetime
    
    
    str_d1 = dt.date.today().strftime('%Y-%m-%d')
    tick = yf3.Ticker(stock)
    
    df3 = pd.DataFrame()
    df3 = tick.earnings_dates
    i = 0
    #print(df3)

    file2 = open('/Users/tarikessawi/data/earnings.csv', 'a')  
    file3 = open('/Users/tarikessawi/data/targets.csv', 'a')  
    
    date_now = tm.strftime('%Y-%m-%d')
    df3 = df3.round(2)
    for index, earnrow in df3.iterrows():
      str_d2 = index.strftime('%Y-%m-%d')
      delta = datetime.strptime(str_d2, "%Y-%m-%d") - datetime.strptime(str_d1, "%Y-%m-%d")
      #earndelta = get_delta_days(datetime.strptime(str_d2, "%Y-%m-%d"),)
      IsEstimateNan = math.isnan(earnrow['EPS Estimate'])
      earndate = index.strftime('%Y-%m-%d')
      if (IsEstimateNan == False and delta.days < 0):  
            backdate = get_dates_back(earndate,15)
            aheaddate = get_dates_ahead(earndate,30)
            writeline = stock,str_d2,delta.days,earnrow['EPS Estimate'],earnrow['Reported EPS'],earnrow['Surprise(%)']
            #print(writeline)
            file2.write(f"{str_d2},{stock},{earnrow['EPS Estimate']},{earnrow['Reported EPS']},{earnrow['Surprise(%)']}\n")
            temp_df = yf.get_data(stock.strip(),start_date=backdate,end_date=aheaddate,interval='1d')
            #histpricedf = pd.concat([histpricedf,temp_df])
            process_stockprice(temp_df,index)
            #print(temp_df)
            #print(f"{writeline}")
            #temp_df = yf.get_data(stock.strip(),start_date=earndate,end_date=aheaddate,interval='1d')
            #process_stockprice(temp_df)
      else:
            pass
            #print("To Far into the future")

            #backdate = date_3_days_back
            #aheaddate = date_now

            #print(f"get-back {get_dates_back(earndate,5)}")
            #print("get-ahead "+get_dates_ahead(earndate,5))

            #temp_df = yf.get_data(stock.strip(),start_date='2024-10-17',end_date=aheaddate,interval='1d')
            #print(temp_df)

            #print(datetime.strptime(get_dates_ahead(earndate,5), "%Y/%m/%d"))
            #file.write(f"{stock},{str_d2},{delta.days},{earnrow['EPS Estimate']},{earnrow['Reported EPS']},{earnrow['Surprise(%)']}\n")
            #file.close


            #get_next_earnings_date2('MMM') 
    #print(histpricedf)
def joindatasets():
  spdf = pd.read_csv('/Users/tarikessawi/data/stockprices.csv')
  eddf = pd.read_csv('/Users/tarikessawi/data/earnings.csv')
  tgdf = pd.read_csv('/Users/tarikessawi/data/targets.csv')
  
  tgdf = tgdf.drop_duplicates()
  eddf = eddf.drop_duplicates()
  
  combineddf = pd.merge(spdf, eddf, on=['Date','Ticker'], how = "outer")
  combineddf = combineddf.drop_duplicates()
  
  combineddf = combineddf.round(2)
  
  os.makedirs('/Users/tarikessawi/data/', exist_ok=True)  
  
  combineddf.to_csv('/Users/tarikessawi/data/combineddf.csv')  
  eddf.to_csv('/Users/tarikessawi/data/earnings.csv')
  tgdf.to_csv('/Users/tarikessawi/data/targets.csv')
  
  
def main():        
    global targetdf
    badlist = ['BRK.B','BF.B','FOX','NWS']
    
    file = open('/Users/tarikessawi/data/stockprices.csv', 'w') 
    file.write(f"Date,Ticker,close,adjclose,days \n")
    
    file2 = open('/Users/tarikessawi/data/earnings.csv', 'w')  
    file2.write(f"Date,Ticker,Est,Rep,Surprise \n")
    
    #file3 = open('/Users/tarikessawi/data/targets.csv', 'w')  
    #file3.write(f"Date,Ticker,Delta,Est,Rep,Surprise \n")
    
    tickers = save_sp500_tickers()
    #df = pd.DataFrame()
    #temp_df = pd.DataFrame()
    i = 0
    for row in tickers:
        row = row.strip()
        i = i+1
        if (row not in badlist):
            try:
                get_targets(row,35)
            except:
                pass
                print("AssertionError")
    
    targetdf.sort_values(by='Delta', inplace=True)
    targetdf.to_csv('/Users/tarikessawi/data/targets.csv')
    
    for index,targetrow in targetdf.iterrows():
      for row in targetrow:
        if (targetrow['Ticker'] not in badlist):
            try:
                get_next_earnings_date(targetrow['Ticker'])
            except:
                pass
    #print(targetdf)
    
    file.close()
    file2.close()
    
    
    joindatasets()
    
    
    
if __name__ == "__main__":
    main()




