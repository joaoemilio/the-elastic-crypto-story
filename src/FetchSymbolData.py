import datetime
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

    _f, day = query_first_and_last_doc(symbol, f"symbols-1m")

    query = {"size": 24*60, "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{day}","lte": f"{ts_end}" ,"format": "strict_date_optional_time"}}}]}},"fields": ["id"], "_source": False}
    ids = []
    if su.es.indices.exists( f"symbols-1m"):
        results = su.es_search("symbols-1m", query)
        if 'hits' in results: results = results['hits']['hits']
        for h in results:
            ids.append(h["_id"])

        if len(results) >= 24*60:
            su.log(f'Already downloaded {su.get_yyyymmdd(day)} s={symbol} cs=1m')
            return {}
        else:
            su.log(f"Download required. s={symbol} day={su.get_yyyymmdd(day)} cs=1m. Missing {len(results)-24*60} docs")

    else:
        su.log(f"Download required. s={symbol} day={su.get_yyyymmdd(day)} cs=1m")
        ids = []

    log(f"Lets fetch {symbol} cs=1m")
    data = {}
    while day < ts_end:
        end_time = day + 24*3600 # in seconds
        pairs = [ (440+25, end_time - 1000*60), (1000, end_time) ] # periods, end_time

        rall = []
        for periods, end_time in pairs:
            logging.info(f"fetching {periods} to {end_time}")
            r = fetch_candles(symbol, day, "1m", periods, end_time=end_time)
            rall += r

        logging.info(f"{len(rall)} lines fetched")

        if rall:
            for o in rall:
                ot = su.get_yyyymmdd_hhmm(o['open_time'])
                _id = f"{symbol}_{ot}_1m"
                
                # do not process if it already exists
                if _id in ids: continue 

                #logging.info(f"Does {_id} exist? {o['open_time_iso']}")
                #if not su.es_exists("symbols-1m", _id):
                o["cs"] = "1m"
                data[_id] = o

        su.log(f'End downloading day {su.get_yyyymmdd(day)} for 1m', 'fetch_candle')
            
        day += 3600*24
    return data

def fetch(symbol:str, cs:str, ts_start, ts_end):
    periods = { "5m": 24*60/5,  "15m": 24*60/15, "1h": 24, "4h": 24/6, "1d": 1 }
    day = ts_start
    _f, day = query_first_and_last_doc(symbol, f"symbols-{cs}")

    query = {"size": periods[cs], "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{day}","lte": f"{ts_end}" ,"format": "strict_date_optional_time"}}}]}}}
    ids = []
    if su.es.indices.exists( f"symbols-{cs}"):
        results = su.es_search(f"symbols-{cs}", query)
        if 'hits' in results: 
            results = results['hits']['hits']
            for h in results:
                ids.append( h["_id"] )

        if len(ids) >= periods[cs]: 
            su.log(f'Already downloaded {su.get_yyyymmdd(day)} s={symbol} cs={cs}')
            return {}
        else:
            su.log(f"Download required. s={symbol} day={su.get_yyyymmdd(day)} cs={cs}. Missing {len(results)-periods[cs]} docs")

    else:
        su.log(f"Download required. s={symbol} day={su.get_yyyymmdd(day)} cs={cs}")
        ids = []


    log(f"Lets fetch {symbol} cs={cs}")
    data = {}
    while day < ts_end:
        su.log(f'Will download {su.get_yyyymmdd(day)} s={symbol} cs={cs}', 'download_candles')

        periods = int((24*3600)/candle_sizes[cs])
        r = fetch_candles(symbol, day, cs, periods)
        if r:
            for o in r:
                ot = su.get_yyyymmdd_hhmm(o['open_time'])
                _id = f"{symbol}_{ot}_{cs}"
                if _id not in ids:
                    o["cs"] = cs
                    data[_id] = o
                else:
                    logging.info(f"Doc {_id} already exists. Skiping")

        day += 3600*24
    return data

def query_first_and_last_doc(symbol: str, iname: str, es="ml-demo"):
    _first = {"size": 1, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": "1569342906", "lte": f"{time.time()}", "format": "strict_date_optional_time"}}}]}}, "fields": ["open_time"], "_source": False}

    _last = {"size": 1, "sort": [{"open_time": {"order": "desc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": "1569342906", "lte": f"{time.time()}", "format": "strict_date_optional_time"}}}]}}, "fields": ["open_time"], "_source": False}

    fd = su.es_search(iname, _first, es)
    ld = su.es_search(iname, _last, es)

    fot = None
    lot = None
    if 'hits' in fd and 'hits' in fd['hits'] and len(fd['hits']['hits']) > 0:
        fot = int(fd['hits']['hits'][0]['fields']['open_time'][0])
    if 'hits' in ld and 'hits' in ld['hits'] and len(ld['hits']['hits']) > 0:
        lot = int(ld['hits']['hits'][0]['fields']['open_time'][0])

    print(f"{lot}\n\n")
    print(f"{fot}\n\n")

    return fot, lot

def main(argv):

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            TimedRotatingFileHandler(f"logs/FetchSymbolData-{su.get_iso_datetime(time.time())}.log",
                                        when="d",
                                        interval=1,
                                        backupCount=7),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logging.info('--------------------------------------------------------------------------------')
    logging.info(f" python3 FetchSymbolData.py 20210801 <-- start from here until the current day")
    logging.info('--------------------------------------------------------------------------------')

    if len(argv) > 0: 
        symbols = su.read_json(f"../config/symbols-group{argv[0]}.json")
    else:
        symbols = su.read_json("symbols.json")
    data = { "symbols-1d": {}, "symbols-4h": {}, "symbols-1h": {}, "symbols-15m": {}, "symbols-5m": {}, "symbols-1m": {} }
    count = 1
    for symbol in symbols:
        end, day = query_first_and_last_doc( symbol, "symbols-1d", "ml-demo")
        if not day:
            day = su.get_ts("20191201")

        end = time.time()

        logging.info(f"start fetching data for {symbol} - {count} of {len(symbols)} FROM {su.get_yyyymmdd_hhmm(day)} TO {su.get_yyyymmdd_hhmm(end)}")
        while day < end:

            data["symbols-1d"] = fetch1d( symbol, day, day+24*3600 )
            data["symbols-4h"] = fetch( symbol, "4h", day, day+24*3600 )
            data["symbols-1h"] = fetch( symbol, "1h", day, day+24*3600 )
            data["symbols-15m"] = fetch( symbol, "15m", day, day+24*3600 )
            data["symbols-5m"] = fetch( symbol, "5m", day, day+24*3600 )
            data["symbols-1m"] = fetch1m(symbol, day, day+24*3600 )

            logging.info(f'Upload {su.get_yyyymmdd(day)} {len(data)} klines for {symbol}.' )
            su.es_bulk_create_multi_index(data,partial=500)

            day += 24*3600
        count += 1

if __name__ == "__main__":
   main(sys.argv[1:])
