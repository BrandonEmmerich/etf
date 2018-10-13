from lxml import etree
import pandas as pd
import psycopg2
import requests
import settings

def get_tickers_seed():
    """A seeding function that gets ETF tickers"""
    url = 'https://seekingalpha.com/etfs-and-funds/etf-tables/countries'
    res = requests.get(url)
    root = etree.HTML(res.content)
    list_of_tickers = root.xpath(settings.XPATH_ETF_TICKER)

    return list_of_tickers

def get_tickers_query(conn):
    '''Get's ETF tickers from the database'''
    cur = conn.cursor()
    cur.execute(settings.QUERY_GET_TICKER)
    list_of_tickers = [tuple_[0] for tuple_ in cur.fetchall()]

    return list_of_tickers

def get_data_and_write_to_db(ticker, list_of_ids, run_id, conn):
    """Gets end of day price data for a given ticker"""
    print ticker

    url = 'https://query1.finance.yahoo.com/v8/finance/chart/%sL?symbol=%s&period1=1280914000&period2=%s&interval=1d&includePrePost=true&events=div%%7Csplit%%7Cearn&lang=en-US&region=US&crumb=YsvQxyK%%2Fz6I&corsDomain=finance.yahoo.com' % (ticker,ticker, run_id)
    res = requests.get(url)
    data = res.json()['chart']['result'][0]
    cur = conn.cursor()

    for i in range(len(data['timestamp'])):
        row = {
            'ticker' : ticker,
            'run_id' : run_id,
            'date_close' : data['timestamp'][i],
            'price_close' : data['indicators']['adjclose'][0]['adjclose'][i],
            'ticker_timestamp_id' : ticker + '_' + str(data['timestamp'][i])
            }

        if row['ticker_timestamp_id'] not in list_of_ids:
            try:
                cur.execute(settings.QUERY_INSERT,row)
                conn.commit()
            except Exception as e:
                print e
                print row['ticker_timestamp_id']
                conn.rollback()

def get_existing_data(conn):
    '''Gets exisiting data so we don't overwrite it'''
    cur = conn.cursor()
    cur.execute(settings.QUERY_GET_DATA)
    list_of_ids = [tuple_[0] for tuple_ in cur.fetchall()]

    return list_of_ids
