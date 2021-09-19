
from logging import error, warn, info
import logging
from logging.handlers import TimedRotatingFileHandler
from os import strerror
import os.path
import time
import ScientistUtils as su
from collections import deque
import sys
import numpy as np

##################################################
# Download
##################################################

candle_sizes = {'1m':60, '5m': 5*60, '15m': 15*60, '1h': 3600, '4h': 4*3600, '1d': 24*3600}

def download_klines(symbol, cs, periods, end_time):
    # end_time in milliseconds

    # prepare empty result
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={cs}&limit={periods}&endTime={end_time}"

    lines = None
    for i in (1,2,3):
        try:
            logging.info(f"trying binance call #{i}")
            lines = su.call_binance(url)
            break
        except ConnectionError as ce:
            error(f"ConnectionError {ce.strerror}")
            logging.info("waiting 5 seconds to call binance again")
            time.sleep(5)


    results = []
    for i2, l in enumerate(lines):
        obj = {
            "symbol": symbol, "open": float(l[1]), "high": float(l[2]), "low": float(l[3]), 
            "close": float(l[4]), "q_volume": float(l[7]), "trades": float(l[8]), "open_time": int(l[0]/1000),
            "open_time_ms": int(l[0]), "open_time_iso": su.get_iso_datetime_sec( int(l[0])/1000 )
        }
        results.append(obj)
    
    return results

def fetch_candles(symbol, day, cs, periods, end_time=None):
    # cs cannot be 1m
    if not end_time:
        end_time = day + 24*3600
    results = download_klines(symbol, cs, periods, end_time*1000 - 1)

    return results

def log(info):
    su.log(info)

##################################################
# 1d Logic
##################################################

def fetch1d( symbol, ts_start, ts_end ):
    day = ts_start
    data = {}

    while day < ts_end:
        logging.info(f'\tprocessing {su.get_yyyymmdd(day)}' )
        ot = su.get_yyyymmdd(day)
        _id = f"{symbol}_{ot}_1d"
        if not su.es_exists("symbols-1d", _id):
            kline = fetch_candles(symbol, day, "1d", 1)
            if kline: 
                obj = kline[0]
                obj["cs"] = "1d"
                data[_id] = obj
      
        day += 3600*24
    return data

def fetch1m(symbol, ts_start, ts_end):

    log(f"Lets fetch {symbol} cs=1m")

    query = {"size": 24*60, "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{ts_start}","lte": f"{ts_end}" ,"format": "strict_date_optional_time"}}}]}}}
    if su.es.indices.exists( f"symbols-1m"):
        results = su.es_search("symbols-1m", query)
        if 'hits' in results: results = results['hits']['hits']
        if len(results) >= 24*60: return {}
    else:
        results = []

    data = {}
    while ts_start < ts_end:
        end_time = ts_start + 24*3600 # in seconds
        pairs = [ (440+25, end_time - 1000*60), (1000, end_time) ] # periods, end_time

        # check if open_time 00:00 and minute 23:59 exists
        id_start = f"{symbol}_{su.get_yyyymmdd_hhmm(ts_start)}_1m"
        id_end = f"{symbol}_{su.get_yyyymmdd_hhmm(end_time)}_1m"
        start_exists = su.es_exists("symbols-1m", id_start)
        end_exists = su.es_exists("symbols-1m", id_end)
        if not start_exists or not end_exists: 
            rall = []
            for periods, end_time in pairs:
                logging.info(f"fetching {periods} to {end_time}")
                r = fetch_candles(symbol, ts_start, "1m", periods, end_time=end_time)
                rall += r

            logging.info(f"{len(rall)} lines fetched")

            if rall:
                for o in rall:
                    ot = su.get_yyyymmdd_hhmm(o['open_time'])
                    _id = f"{symbol}_{ot}_1m"
                    
                    # do not process if it already exists
                    if _id in results: continue 

                    #logging.info(f"Does {_id} exist? {o['open_time_iso']}")
                    #if not su.es_exists("symbols-1m", _id):
                    o["cs"] = "1m"
                    data[_id] = o
            su.log(f'End downloading day {su.get_yyyymmdd(ts_start)} for 1m', 'fetch_candle')
        else:
            su.log(f'Day is downloaded {su.get_yyyymmdd(ts_start)} for 1m. Skipping', 'fetch_candle')
            
        ts_start += 3600*24
    return data

def fetch(symbol:str, cs:str, ts_start, ts_end):
    periods = { "5m": 24*60/5,  "15m": 24*60/15, "1h": 24, "4h": 24/6, "1d": 1 }

    log(f"Lets fetch {symbol} cs={cs}")
    day = ts_start

    query = {"size": periods[cs], "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{ts_start}","lte": f"{ts_end}" ,"format": "strict_date_optional_time"}}}]}}}
    if su.es.indices.exists( f"symbols-{cs}"):
        results = su.es_search(f"symbols-{cs}", query)
        if 'hits' in results: 
            results = results['hits']['hits']

        if len(results) >= periods[cs]: 
            return {}          
    else:
        results = []

    data = {}
    while day < ts_end:
        su.log(f'Will download {su.get_yyyymmdd(day)} cs={cs}', 'download_candles')

        periods = int((24*3600)/candle_sizes[cs])
        r = fetch_candles(symbol, day, cs, periods)
        if r:
            for o in r:
                ot = su.get_yyyymmdd_hhmm(o['open_time'])
                _id = f"{symbol}_{ot}_{cs}"
                if _id not in results:
                    o["cs"] = cs
                    data[_id] = o
                else:
                    logging.info(f"Doc {_id} already exists. Skiping")

        day += 3600*24
    return data

def main(argv):

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            TimedRotatingFileHandler(f"logs/FetchSymbolData.log",
                                        when="d",
                                        interval=1,
                                        backupCount=7),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logging.info('--------------------------------------------------------------------------------')
    logging.info(f" python3 FetchSymbolData.py BTCUSDT 20210801 [20210901] <-- only BTCUSDT from start to [end]")
    logging.info(f" python3 FetchSymbolData.py ALL 20210801 [20210901]<-- ALL symbols in symbols.json from start to [end]")
    logging.info('--------------------------------------------------------------------------------')

    start = su.get_ts(argv[0])
    day = start
    end = su.get_ts(argv[1])

    symbols = su.read_json("symbols.json")

    symbols_1d = {}
    data = { "symbols-1d": {}, "symbols-4h": {}, "symbols-1h": {}, "symbols-15m": {}, "symbols-5m": {}, "symbols-1m": {} }
    while day < end:
        count = 1
        for symbol in symbols:
            logging.info(f"start fetching data for {symbol} - {count} of {len(symbols)}")
            count += 1

            data["symbols-1d"] = fetch1d( symbol, day, day+24*3600 )
            data["symbols-4h"] = fetch( symbol, "4h", day, day+24*3600 )
            data["symbols-1h"] = fetch( symbol, "1h", day, day+24*3600 )
            data["symbols-15m"] = fetch( symbol, "15m", day, day+24*3600 )
            data["symbols-5m"] = fetch( symbol, "5m", day, day+24*3600 )
            data["symbols-1m"] = fetch1m(symbol, day, day+24*3600 )

            logging.info(f'Upload {su.get_yyyymmdd(day)} for {symbol}.' )
            su.es_bulk_create_multi_index(data,partial=500)

        day += 24*3600

if __name__ == "__main__":
   main(sys.argv[1:])
