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
        print(i)
        if i > 10:
          break
    return tickers


def main():
    tickers = save_sp500_tickers()

if __name__ == "__main__":
    main()




