import logging
from logging.handlers import TimedRotatingFileHandler
import ScientistUtils as su
import numpy as np
from collections import deque
import time, sys

def moving_avg(number, numbers, window_size):
    numbers.append( number )
    n = numbers[-window_size:]
    mma = sum(n) / window_size
    return mma

def delta(starting_price, closing_price):
    return (closing_price - starting_price)/starting_price

def std_dev(number, numbers, window_size):
    numbers.append( number )
    n = numbers[-window_size:]
    return np.std(n)

def mean(number, numbers, window_size):
    numbers.append( number )
    n = numbers[-window_size:]
    return np.mean(n)

def dp( current_price, previous_price, ):
    return 100*(current_price - previous_price)/previous_price

def bb( current_price, mov_avg_price, std_price ):
    return ( current_price - mov_avg_price) / std_price

def enrich1m(symbol, ts_start, ts_end):

    window_size = 200
    ts_window_size = ts_start-200*60

    query = {"size": window_size, "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{ts_window_size}","lte": f"{ts_start}" ,"format": "strict_date_optional_time"}}}]}}}
    data = su.es_search("symbols", query)['hits']['hits']

    q = deque()
    for d in data:
        _id = d['_id']
        if 'USDT' not in _id: continue
        s = d['_source']
        q.append( { "_id": _id, "close": s['close'] } )

    closes = [ v['close'] for v in q ]

    logging.info(f"{su.get_yyyymmdd_hhmm(time.time())} Lets enrich {symbol} cs=1m")
    minute = ts_start

    mms = [5, 7, 9, 10, 15, 20, 21, 25, 51, 99, 200]
    data = {}
    while minute < ts_end:
        _id = f"{symbol}_{su.get_yyyymmdd_hhmm(minute)}_1m"
        doc = su.es_get( "symbols", _id )['_source']
        close = doc['close']
        if "1m" not in doc: doc["1m"] = {}
        if "aug" in doc and doc["aug"]["1m"] == "1.0.0": 
            print(f"{_id} already augmented 1m: {doc['aug']['1m']}")
        else:
            for mm in mms:
                doc["1m"][f"close_mm{mm}"] = moving_avg( close, closes[-mm:] , mm)
                doc["1m"][f"std{mm}"] = std_dev( close, closes[-mm:], mm )
                doc["1m"][f"mid_bb{mm}"] = mean( close, closes[-mm:], mm )
                doc["1m"][f"bb{mm}"] = bb(close, doc["1m"][f"close_mm{mm}"], doc["1m"][f"std{mm}"] )

            doc["1m"]["dp"] = dp( close, closes[-1])
            doc["1m"]['d0'] = delta( doc['open'], doc['close'] )
            doc["aug"] = { "1m": "1.0.0" }

            q.append(close)
            q.popleft()

            data[_id] = doc
            if len(data) == 1000:
                logging.info(f"{su.get_yyyymmdd_hhmm(time.time())} uploading 1000 docs. Last ID: {_id}")
                su.es_bulk_update(iname="symbols", data=data, partial=1000)
                data = {}

        minute += 60

    su.es_bulk_update(iname="symbols", data=data, partial=1000)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        TimedRotatingFileHandler(f"logs/EnrichSymbolData.log",
                                    when="d",
                                    interval=1,
                                    backupCount=7),
        logging.StreamHandler(sys.stdout)
    ]
)

enrich1m( "BTCUSDT", 1630454400, 1631836800 )

# query = su.read_json(f"queries/symbol_1m_range_open_time.json")
# print( query['query'])
# query['query']['bool']['filter'][0]['bool']['should'][0]['match_phrase']['symbol.keyword'] = "ETHUSDT"
# query['query']['bool']['filter'][1]['range']['open_time']['gte'] = "1627784500"
# query['query']['bool']['filter'][1]['range']['open_time']['lte'] = "1627786700"

# results = su.es_search("symbols", query)
# print(results['hits']['hits'])

'''
kline1m = {
    "symbol": "TKOUSDT",
    "open": 2.173,
    "high": 2.173,
    "low": 2.171,
    "close": 2.171,
    "q_volume": 283.7748,
    "trades": 3,
    "open_time": 1631750340,
    "open_time_ms": 1631750340000,
    "open_time_iso": "2021-09-15T23:59:00",
    "cs": "1m",
    "1m" : { "mm5": 2.173, "mm7": 2.169 }, 
    "aug" : { 
        "1m" : { "mm5": 0, "mm7": 0 } ,
        "5m": { "mm5": 0, "mm7": 0 }
    }
  }


t = time.time()
n = [1, 4, 5, 10, 8, 7, 2, 3, 1, 5, 1, 4, 5, 10, 8, 7, 2, 3, 1, 5, 3, 2, 12, 3, 6, 78, 9, 7, 5, 3]
ws = 10
print(f"Calculate mma")
i = 0
while i < 10000:
    mma = moving_avg(50, n, ws)
    #print(f"Moving avg: {mma}")
    i += 1

print( f"Time spent: {time.time()-t}s")
'''