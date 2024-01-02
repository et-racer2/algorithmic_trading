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
import matplotlib.pyplot as plt 
import numpy as np 
from polygon import RESTClient
import requests


av_api_key = 'UZNKFIAR5CNH759X'

def getnews(ticker):
  #Free Key
  #av_api_key = 'GE6LE6L7N5GDSM51'
  #Premium Key
  
# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
  url = (f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={av_api_key}&limit=1&time_from=20231220T0000")
  r = requests.get(url)
  json_data = r.json()
  
  # Extract data from JSON to a DataFrame
  df_feed = pd.DataFrame(json_data['feed'])

  # Normalize the 'topics' column
  df_topics = pd.json_normalize(df_feed['topics']).add_prefix('topic_')

  # Concatenate the dataframes along columns
  df = pd.concat([df_feed, df_topics], axis=1).drop('topics', axis=1)

  # Display the DataFrame
  print(df)
  #print(df.info())
  filename = (f"/Users/tarikessawi/data/{ticker}-stock_news.csv")
  df.to_csv(filename,mode='a')  
  
  file2 = open(filename, 'a')  
  
  for index, row in df.iterrows():
    print(row)
    #print(f"{index}, {row['ticker_sentiment.ticker']}\n")
    ticker_sentiment_ticker = row['ticker_sentiment']
    print(f"{ticker_sentiment_ticker}\n")
    index = 0
    for item in ticker_sentiment_ticker:
       if ticker_sentiment_ticker[index].get('ticker') == ticker:
          file2.write(f"{index}, ticker: {ticker_sentiment_ticker[index].get('ticker')} - ")
          file2.write(f"{index}, relevance_score: {ticker_sentiment_ticker[index].get('relevance_score')} - ")
          file2.write(f"{index}, ticker_sentiment_score: {ticker_sentiment_ticker[index].get('ticker_sentiment_score')} - ")
          file2.write(f"{index}, ticker_sentiment_label: {ticker_sentiment_ticker[index].get('ticker_sentiment_label')}\n")
          file2.write(f"{index}, title: {row['title']}\n")
          file2.write(f"{index}, url]: {row['url']}\n")
          return
       index = index+1
          
  file2.close()        
    #print(f"{index}, {row['title']}\n,{row['url']},{row['time_published']},{row['summary']}\n")
def fetch_daily_adjusted_prices(ticker, av_api_key): 
  targetdf = pd.DataFrame()
  base_url = 'https://www.alphavantage.co/query'
  params = {
      'function': 'TIME_SERIES_DAILY_ADJUSTED',
      'symbol': ticker,
      'apikey': av_api_key,
      'datatype': 'json'  # Data format (can also be 'csv')
  }
  try:
      response = requests.get(base_url, params=params)
      data = response.json()
      #print(data)
      for key, value in data.items():
        if key == 'Time Series (Daily)':
          targetdf = pd.DataFrame.from_dict(value, orient='index')
          print(targetdf)
          filename = (f"/Users/tarikessawi/data/{ticker}-fulldump.csv")
          targetdf.to_csv(filename,mode='a')
          
          #for date, details in value.items():
            #print(f"Date: {date} , {details}")
            #targetdf = pd.DataFrame.from_dict(details, orient='index').T
            #print(targetdf)
          #targetdf.loc[len(targetdf)] = new_row
          #for key, value in details.items():
          # print(f"{date} - {key}: {value}")
  except requests.RequestException as e:
    print("Request Error:", e)
    
def getallinfo():
  # Set your Polygon.io API key
  api_key = 'LgNAcz4laFfNgR1VQk2bEPgEm4c7DHrP'

  # Create a REST client instance
  #client = RESTClient(api_key)

  # Get the last quote for a specific stock
  #symbol = 'AAPL'
  #ticker = 'AAPL'
  #response = client.stocks_last_quote(symbol)
  #response = client.get_ticker_details(symbol)

  #response = client.get_summaries(symbol)
 # print(f"Last trade price for {symbol}: {response}")
  

  # List Aggregates (Bars)
  aggs = []
  for a in client.list_aggs(ticker=ticker, multiplier=1, timespan="day", from_="2023-01-01", to="2023-06-13", limit=50000):
      print(a)
      #aggs.append(a)

  #print(aggs)

  # Get Last Trade
  #trade = client.get_last_trade(ticker=ticker)
  #print(trade)

  # List Trades
  #trades = client.list_trades(ticker=ticker, timestamp="2022-01-04")
  #for trade in trades:
      #print(trade)

  # Get Last Quote
  #quote = client.get_last_quote(ticker=ticker)
  #print(quote)

  # List Quotes
  #quotes = client.list_quotes(ticker=ticker, timestamp="2022-01-04")
  #for quote in quotes:
  #    print(quote)
 
 
# Function to fetch daily adjusted stock prices from Alpha Vantage

def fetch_fundamental_data(ticker, av_api_key):
  base_url = 'https://www.alphavantage.co/query'
  params = {
      'function': 'OVERVIEW',  # Company overview function
      'symbol': ticker,
      'apikey': av_api_key
  }

  try:
      response = requests.get(base_url, params=params)
      fundamental_data = response.json()
      df = pd.DataFrame.from_dict(fundamental_data, orient='index').T
      for key, value in fundamental_data.items():
          print(f"fundamental data is : {key}: {value}")
      # Check if the request was successful
      if 'Error Message' in fundamental_data:
          print("Error:", data['Error Message'])
          return None
      else:
          return fundamental_data
  except requests.RequestException as e:
      print("Request Error:", e)
      return None


        
# Display the fetched fundamental data
  
def analyzedata():
  spdf = pd.read_csv('/Users/tarikessawi/data/stockprices.csv')
  eddf = pd.read_csv('/Users/tarikessawi/data/earnings.csv')
  tgdf = pd.read_csv('/Users/tarikessawi/data/targets.csv')
  cmdf = pd.read_csv('/Users/tarikessawi/data/combineddf.csv',usecols=['Key','Date','Ticker','close','adjclose','days ','Est','Rep','Surprise '])	
  
  
  tgdf = tgdf.drop_duplicates()
  eddf = eddf.drop_duplicates()
  cmdf = cmdf.drop_duplicates()

  #print(cmdf)
  #print(cmdf)
  cmdf.to_csv('/Users/tarikessawi/data/dedupe-combineddf.csv')  
  #eddf.to_csv('/Users/tarikessawi/data/earnings.csv')
  #tgdf.to_csv('/Users/tarikessawi/data/targets.csv')
  # creating two array for plotting 
  #print(cmdf['days '].fillna(-20).unique())
  
  # Assuming 'column_name' is the name of the column in your DataFrame
  unique_values = np.sort(cmdf['days '].fillna(-20).unique())
  print(unique_values)

  # Assuming 'df' is the name of your DataFrame
  filtered_df1 = cmdf[cmdf['Key'] == '2023-10-05-LW']  # Filter rows based on a condition for series 1
  filtered_df2 = cmdf[cmdf['Key'] == '2023-07-25-LW']   # Filter rows based on a condition for series 2
  filtered_df3 = cmdf[cmdf['Key'] == '2023-04-06-LW']   # Filter rows based on a condition for series 2
  filtered_df4 = cmdf[cmdf['Key'] == '2023-01-05-LW']   # Filter rows based on a condition for series 2
  

  plt.plot(filtered_df1['days '], filtered_df1['close'], label='2023-10-05-LW')
  plt.plot(filtered_df2['days '], filtered_df2['close'], label='2023-07-25-LW')
  plt.plot(filtered_df3['days '], filtered_df3['close'], label='2023-04-06-LW')
  plt.plot(filtered_df4['days '], filtered_df4['close'], label='2023-01-05-LW')

  # Add labels and title
  plt.xlabel('X-axis label')
  plt.ylabel('Y-axis label')
  plt.title('Plot of column1 and column2 (Filtered)')

  # Add legend
  plt.legend()

  # Add a vertical line at a specific x-axis point
  plt.axvline(x=0, color='red', linestyle='--')


  # Display the plot
  plt.show()  
  q1 = '2023-01-24-MSFT'
  q2 = '2023-04-25-MSFT'
  q3 = '2023-07-25-MSFT'
  q4 = '2023-10-24-MSFT'
      
  filtered_df1 = cmdf[cmdf['Key'] == q1]  # Filter rows based on a condition for series 1
  filtered_df2 = cmdf[cmdf['Key'] == q2]   # Filter rows based on a condition for series 2
  filtered_df3 = cmdf[cmdf['Key'] == q3]   # Filter rows based on a condition for series 2
  filtered_df4 = cmdf[cmdf['Key'] == q4]   # Filter rows based on a condition for series 2


  plt.plot(filtered_df1['days '], filtered_df1['close'], label=q1)
  plt.plot(filtered_df2['days '], filtered_df2['close'], label=q2)
  plt.plot(filtered_df3['days '], filtered_df3['close'], label=q3)
  plt.plot(filtered_df4['days '], filtered_df4['close'], label=q4)

# Add a vertical line at a specific x-axis point
  plt.axvline(x=0, color='red', linestyle='--')

  # Add labels and title
  plt.xlabel('X-axis label')
  plt.ylabel('Y-axis label')
  plt.title('Plot of column1 and column2 (Filtered)')

  # Add legend
  plt.legend()

  # Display the plot
  plt.show()  

  
def main():    
  #getallinfo()    
  #analyzedata() 
  getnews('LW')
  #fetch_fundamental_data('LW', av_api_key)
  fetch_daily_adjusted_prices('LW', av_api_key)  



    
if __name__ == "__main__":
    main()




