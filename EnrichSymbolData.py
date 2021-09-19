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


def get_closes_1m( symbol, ts_start, window_size):
    ts_window_size = ts_start-window_size*60

    query = {"size": window_size, "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{ts_window_size}","lte": f"{ts_start}" ,"format": "strict_date_optional_time"}}}]}}}
    data = su.es_search("symbols-1m", query)['hits']['hits']

    q_closes = deque()
    q_volumes = deque()
    q_trades = deque()
    for d in data:
        _id = d['_id']
        if 'USDT' not in _id: continue
        s = d['_source']
        q_closes.append( s['close'] )
        q_volumes.append( s['q_volume'] ) 
        q_trades.append( s['trades'] ) 

    return q_closes, q_volumes, q_trades


def get_closes( symbol, cs, ts_start, window_size):
    periods = { "5m": 60*5,  "15m": 60*15, "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60 }
    ts_window_size = ts_start-window_size*periods[cs]

    query = {"size": window_size, "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{ts_window_size}","lte": f"{ts_start}" ,"format": "strict_date_optional_time"}}}]}}}
    data = su.es_search(f"symbols-{cs}", query)['hits']['hits']

    q_closes = deque()
    q_volumes = deque()
    q_trades = deque()
    for d in data:
        _id = d['_id']
        if 'USDT' not in _id: continue
        s = d['_source']
        q_closes.append( s['close'] )
        q_volumes.append( s['q_volume'] ) 
        q_trades.append( s['trades'] ) 

    return q_closes, q_volumes, q_trades

def enrich1m(symbol, data, ts_start, ts_end):

    q_closes, q_volumes, q_trades = get_closes_1m( symbol, ts_start, 200 )

    minute = ts_start
    mms = [5, 7, 9, 10, 15, 20, 21, 25, 51, 99, 200]
    aug = {}
    while minute < ts_end:
        _id = f"{symbol}_{su.get_yyyymmdd_hhmm(minute)}_1m"

        try:
            doc = data[_id]
            close = doc['close']
            if "1m" not in doc: doc["1m"] = {}
            if "aug" in doc and doc["aug"]["1m"] == "1.0.0": 
                logging.debug(f"{_id} already augmented 1m: {doc['aug']['1m']}")
            else:
                for mm in mms:
                    if mm <= len(q_closes):
                        doc["1m"][f"close_mm{mm}"] = moving_avg( close, list(q_closes)[-mm:] , mm)
                        doc["1m"][f"trades_mm{mm}"] = moving_avg( list(q_volumes)[-1], list(q_trades)[-mm:], mm)
                        doc["1m"][f"q_volume_mm{mm}"] = moving_avg( list(q_volumes)[-1], list(q_volumes)[-mm:], mm)
                        doc["1m"][f"std{mm}"] = std_dev( close, list(q_closes)[-mm:], mm )
                        doc["1m"][f"mid_bb{mm}"] = mean( close, list(q_closes)[-mm:], mm )
                        doc["1m"][f"bb{mm}"] = bb(close, doc["1m"][f"close_mm{mm}"], doc["1m"][f"std{mm}"] )
                    else:
                        doc["1m"][f"close_mm{mm}"] = 0
                        doc["1m"][f"trades_mm{mm}"] = 0
                        doc["1m"][f"q_volume_mm{mm}"] = 0
                        doc["1m"][f"std{mm}"] = 0
                        doc["1m"][f"mid_bb{mm}"] = 0
                        doc["1m"][f"bb{mm}"] = 0

                doc["1m"]["dp"] = dp( close, q_closes[-1])
                doc["1m"]['d0'] = delta( doc['open'], doc['close'] )
                doc["aug"] = { "1m": "1.0.0" }

                q_closes.append(close)
                q_closes.popleft()

                aug[_id] = doc
                if len(aug) == 1000:
                    logging.info(f"{su.get_yyyymmdd_hhmm(time.time())} uploading 1000 docs. Last ID: {_id}")
                    su.es_bulk_update(iname="symbols-1m", data=aug, partial=1000)
                    aug = {}
        except Exception as ex:
            logging.error(ex)

        minute += 60

    su.es_bulk_update(iname="symbols-1m", data=aug, partial=1000)

def enrich(symbol, cs, data, ts_start, ts_end):

    logging.info(f"Fetching {cs} closes for {symbol} from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(ts_end)}")
    q_closes, q_volumes, q_trades = get_closes( symbol, cs, ts_start, 200 )

    periods = { "5m": 60*5,  "15m": 60*15, "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60 }
    minute = ts_start
    mms = [5, 7, 9, 10, 15, 20, 21, 25, 51, 99, 200]
    doc_cs = {}
    doc_1m = {}
    first = True
    while minute < ts_end:
        _id = f"{symbol}_{su.get_yyyymmdd_hhmm(minute)}_1m"

        if minute % periods[cs] == 0:
            if cs == "1d":
                _id_cs = f"{symbol}_{su.get_yyyymmdd(minute)}_{cs}"
            else:
                _id_cs = f"{symbol}_{su.get_yyyymmdd_hhmm(minute)}_{cs}"

            doc_cs = su.es_get(f"symbols-{cs}", _id_cs)
            if "_source" in doc_cs:
                doc_cs = doc_cs['_source']
            if _id in data:
                doc_1m = data[_id]
                doc_1m[f"is_{cs}"] = 1

        if _id in data: 
            doc_1m = data[_id]  #su.es_get("symbols-1m", _id)
            if f"is_{cs}" not in doc_1m: doc_1m[f"is_{cs}"] = 0
            close_1m = doc_1m['close']
            close_cs = doc_cs['close']
            q_vol_cs = doc_cs['q_volume']
            trades_cs = doc_cs['trades']
            
            if cs not in doc_1m: doc_1m[cs] = {}
            if "aug" not in doc_1m: doc_1m["aug"] = {}
            for mm in mms:
                if mm <= len(q_closes):
                    doc_cs[f"close_mm{mm}"] = moving_avg( close_1m, list(q_closes)[-mm:], mm)
                    doc_cs[f"trades_mm{mm}"] = moving_avg( trades_cs, list(q_trades)[-mm:], mm)
                    doc_cs[f"q_volume_mm{mm}"] = moving_avg( q_vol_cs, list(q_volumes)[-mm:], mm)
                    doc_cs[f"std{mm}"] = std_dev( close_1m, list(q_closes)[-mm:], mm )
                    doc_cs[f"mid_bb{mm}"] = mean( close_1m, list(q_closes)[-mm:], mm )
                    doc_cs[f"bb{mm}"] = bb(close_1m, doc_cs[f"close_mm{mm}"], doc_cs[f"std{mm}"] )
                else:
                    doc_cs[f"close_mm{mm}"] = 0
                    doc_cs[f"std{mm}"] = 0
                    doc_cs[f"mid_bb{mm}"] = 0
                    doc_cs[f"bb{mm}"] = 0

            doc_cs["dp"] = dp( close_1m, doc_cs['close'] )
            doc_cs['d0'] = delta( doc_cs['open'], doc_cs['close'] )
            doc_cs['q_volume_d0'] = delta( q_volumes[-1], q_vol_cs )
            doc_cs['trades_d0'] = delta( q_trades[-1], trades_cs )
            doc_1m[cs] = doc_cs
            doc_1m["aug"] = { cs: "1.0.0" }

            # future prices => low, high, close <==> 5m | 15m | 30m | 1h | 2h | 4h | 8h | 12h | 24h 
            prices = { "5m": 60*5,  "15m": 60*15,  "30m": 60*30, "1h": 60*60, "2h": 2*60*60, "4h": 4*60*60, "8h": 8*60*60, "12h": 12*60*60, "24h": 24*60*60 }
            for p in prices:
                id_p = f"{symbol}_{su.get_yyyymmdd_hhmm(minute+prices[p])}_1m"
                doc_p = data[id_p]
                if "future" not in doc_1m: doc_1m["future"] = {}
                doc_1m["future"][p] = { 
                    "low":   { "p": doc_p["low"], "d": delta( doc_1m['close'], doc_p["low"] ) }, 
                    "close": { "p": doc_p["close"], "d": delta( doc_1m['close'], doc_p["close"] ) },
                    "high":  { "p": doc_p["high"], "d": delta( doc_1m['close'], doc_p["high"] ) }
                }

            if not first and (minute % periods[cs] == 0):
                q_closes.append(close_cs)
                q_closes.popleft()
                q_volumes.append(q_vol_cs)
                q_volumes.popleft()
                q_trades.append(trades_cs)
                q_trades.popleft()

            data[_id] = doc_1m
        else:
            logging.error(f"{_id} not found")

        if first: first = False
        minute += 60

    return data

def enrichDay(symbol, day):
    ts_start = day
    ts_end = ts_start + 24*3600
    ts_aug_end = ts_start + 60*3600

    query = {"size": 72*60 ,"query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{ts_start}","lte": f"{ts_aug_end}" ,"format": "strict_date_optional_time"}}}]}}}
    results = su.es_search("symbols-1m", query)['hits']['hits']
    data = {}
    for d in results:
        doc = d['_source']
        data[d['_id']] = doc

    # print([k for k in data])

    #logging.info(f"enriching {len(data)} of {symbol} from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(ts_end)}")
    #enrich1m(symbol, data, ts_start, ts_end)
    # data = { "symbols-1d": {}, "symbols-4h": {}, "symbols-1h": {}, "symbols-15m": {}, "symbols-5m": {}, "symbols-1m": {} }

    logging.info(f"enriching {len(data)} of {symbol} 1d from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(ts_end)}")
    data = enrich(symbol, "1d", data, ts_start, ts_end)
    logging.info(f"enriching {len(data)} of {symbol} 4h from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(ts_end)}")
    data = enrich(symbol, "4h", data, ts_start, ts_end)
    logging.info(f"enriching {len(data)} of {symbol} 1h from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(ts_end)}")
    data = enrich(symbol, "1h", data, ts_start, ts_end)
    logging.info(f"enriching {len(data)} of {symbol} 15m from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(ts_end)}")
    data = enrich(symbol, "15m", data, ts_start, ts_end)
    logging.info(f"enriching {len(data)} of {symbol} 5m from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(ts_end)}")
    data = enrich(symbol, "5m", data, ts_start, ts_end)
    
    logging.info(f"Sending {len(data)} of {symbol} to Elastic Cloud")
    su.es_bulk_update("symbols-1m", data, partial=500 )

def main(argv):

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

    logging.info('--------------------------------------------------------------------------------')
    logging.info(f" python3 EnrichSymbolData.py BTCUSDT 20210801 [20210901] <-- only BTCUSDT from start to [end]")
    logging.info(f" python3 EnrichSymbolData.py ALL 20210801 [20210901]<-- ALL symbols in symbols.json from start to [end]")
    logging.info('--------------------------------------------------------------------------------')

    start = su.get_ts(argv[1])
    day = start
    end = su.get_ts(argv[2])
    if argv[0] == "ALL":
        symbols = su.read_json("symbols.json")
        while day < end:
            for symbol in symbols:
                logging.info(f"start fetching data from day {su.get_iso_datetime(day)} for {symbol}")
                enrichDay( symbol, day )

            day += 24*3600
    else:
        symbol = argv[0]
        while day < end:
            logging.info(f"start fetching data from day {su.get_iso_datetime(day)} for {symbol}")
            enrichDay( symbol, day )

            day += 24*3600

if __name__ == "__main__":
   main(sys.argv[1:])



# query = su.read_json(f"queries/symbol_1m_range_open_time.json")
# print( query['query'])
# query['query']['bool']['filter'][0]['bool']['should'][0]['match_phrase']['symbol.keyword'] = "ETHUSDT"
# query['query']['bool']['filter'][1]['range']['open_time']['gte'] = "1627784500"
# query['query']['bool']['filter'][1]['range']['open_time']['lte'] = "1627786700"

# results = su.es_search("symbols-1m", query)
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