import urllib2
import requests
import time
import datetime
import json
import utils
import os

def getTicker():
    url = "https://poloniex.com/public?command=returnTicker"
    req = urllib2.Request(url)
    ticker = urllib2.urlopen(req).read()
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    ticker_json = json.loads(ticker)
    btc_json = ticker_json['USDT_BTC']
    btc_json['timestamp'] = st
    print btc_json
    return btc_json

if __name__ == '__main__':
    credentials = utils.get_credentials()
    bigquery = utils.create_bigquery_client(credentials)
    ticker = getTicker()
    response = utils.bq_data_insert_row(bigquery, os.environ['PROJECT_ID'], os.environ['BQ_DATASET'], os.environ['BQ_TABLE'], ticker)
    print response
