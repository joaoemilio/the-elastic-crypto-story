import logging
from logging.handlers import TimedRotatingFileHandler
import ScientistUtils as su
import numpy as np
from collections import deque
import time
import sys
import os


def moving_avg(number, numbers, window_size):
    if window_size == 0:
        return 0
    numbers.append(number)
    n = numbers[-window_size:]
    mma = sum(n) / window_size
    return mma


def delta(starting_price, closing_price):
    return 0 if starting_price == 0 else (closing_price - starting_price)/starting_price


def std_dev(number, numbers, window_size):
    numbers.append(number)
    n = numbers[-window_size:]
    return np.std(n)


def mean(number, numbers, window_size):
    numbers.append(number)
    n = numbers[-window_size:]
    return np.mean(n)


def dp(current_price, previous_price, ):
    return 0 if previous_price == 0 else 100*(current_price - previous_price)/previous_price


def bb(current_price, mov_avg_price, std_price):
    return 0 if std_price == 0 else (current_price - mov_avg_price) / std_price


def get_closes(symbol, cs, ts_start, window_size):
    periods = {"5m": 60*5,  "15m": 60*15,
               "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60}
    ts_window_size = ts_start-window_size*periods[cs]

    query = {"size": window_size, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {
        "range": {"open_time": {"gte": f"{ts_window_size}", "lte": f"{ts_start}", "format": "strict_date_optional_time"}}}]}}}
    data = su.es_search(f"symbols-{cs}", query)['hits']['hits']

    q_closes = deque()
    q_volumes = deque()
    q_trades = deque()
    for d in data:
        _id = d['_id']
        if 'USDT' not in _id:
            continue
        s = d['_source']
        q_closes.append(s['close'])
        q_volumes.append(s['q_volume'])
        q_trades.append(s['trades'])

    return q_closes, q_volumes, q_trades

def enrich(symbol, cs, data, doc_cs, q_closes, q_volumes, q_trades):
    close_cs = doc_cs['close']
    q_vol_cs = doc_cs['q_volume']
    trades_cs = doc_cs['trades']
    mms = [5, 7, 9, 10, 15, 20, 21, 25, 51, 99, 200]
    for mm in mms:
        if mm <= len(q_closes):
            doc_cs[f"close_mm{mm}"] = moving_avg(
                close_cs, list(q_closes)[-mm:], mm)
            doc_cs[f"trades_mm{mm}"] = moving_avg(
                trades_cs, list(q_trades)[-mm:], mm)
            doc_cs[f"q_volume_mm{mm}"] = moving_avg(
                q_vol_cs, list(q_volumes)[-mm:], mm)
            doc_cs[f"std{mm}"] = std_dev(
                close_cs, list(q_closes)[-mm:], mm)
            doc_cs[f"mid_bb{mm}"] = mean(
                close_cs, list(q_closes)[-mm:], mm)
            doc_cs[f"bb{mm}"] = bb(
                close_cs, doc_cs[f"close_mm{mm}"], doc_cs[f"std{mm}"])
            doc_cs[f'd_vol_{mm}'] = delta(
                doc_cs['q_volume'], doc_cs[f'q_volume_mm{mm}'])
            doc_cs[f'd_trades_{mm}'] = delta(
                doc_cs['trades'], doc_cs[f'trades_mm{mm}'])
        else:
            doc_cs[f"close_mm{mm}"] = 0
            doc_cs[f"trades_mm{mm}"] = 0
            doc_cs[f"q_volume_mm{mm}"] = 0
            doc_cs[f"std{mm}"] = 0
            doc_cs[f"mid_bb{mm}"] = 0
            doc_cs[f"bb{mm}"] = 0
            doc_cs[f'd_vol_{mm}'] = 0
            doc_cs[f'd_trades_{mm}'] = 0

    doc_cs["dp"] = dp(doc_cs['close'], q_closes[-1])
    #print(f"close_1m={doc_1m['close']} close_cs={doc_cs['close']} dp={doc_cs['dp']} ")
    doc_cs['d0'] = delta(doc_cs['open'], doc_cs['close'])
    if len(q_volumes) > 0 and len(q_trades) > 0:
        doc_cs['q_volume_d0'] = delta(q_volumes[-1], q_vol_cs)
        doc_cs['trades_d0'] = delta(q_trades[-1], trades_cs)
    else:
        doc_cs['q_volume_d0'] = 0
        doc_cs['trades_d0'] = 0

    # future prices => low, high, close <==> 5m | 15m | 30m | 1h | 2h | 4h | 8h | 12h | 24h
    prices = {"5m": 60*5,  "15m": 60*15,  "30m": 60*30, "1h": 60*60, "2h": 2 *
                60*60, "4h": 4*60*60, "8h": 8*60*60, "12h": 12*60*60, "24h": 24*60*60}
    for p in prices:
        id_p = f"{symbol}_{su.get_yyyymmdd_hhmm(doc_cs['open_time']+prices[p])}_{cs}"
        if id_p in data:
            doc_p = data[id_p]
            if "future" not in doc_cs:
                doc_cs["future"] = {}
            doc_cs["future"][p] = {
                "low":   {"p": doc_p["low"], "d": delta(doc_cs['close'], doc_p["low"])},
                "close": {"p": doc_p["close"], "d": delta(doc_cs['close'], doc_p["close"])},
                "high":  {"p": doc_p["high"], "d": delta(doc_cs['close'], doc_p["high"])}
            }
        
    aug = doc_cs.copy()
    aug["version"] = "1.0.0"

    return aug

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

    return fot, lot

def get_augmentation_period(symbol: str, cs: str):
    start_cs, end_cs = query_first_and_last_doc( symbol, f"symbols-{cs}", "ml-demo")
    if not start_cs:
        start_cs = su.get_ts("20191201")
    if not end_cs:
        end_cs = time.time()

    logging.info(
        f"{symbol} downloaded start={su.get_iso_datetime(start_cs)} end={su.get_iso_datetime(end_cs)}")

    if su.es.indices.exists( f"symbols-aug-{cs}"):
        start_aug, end_aug = query_first_and_last_doc( symbol, f"symbols-aug-{cs}", "ml-demo")
        if not end_aug:
            day = start_cs
        else:
            day = end_aug
    else:
        day = start_cs

    print(f"AUGMENT {symbol} FROM start={su.get_iso_datetime(day)} TO end={su.get_iso_datetime(end_cs)}")
    return day, end_cs

def main(argv):

    symbol = argv[0]

    group = None
    if symbol == "ALL":
        symbols = su.get_symbols()
    elif "GROUP" in symbol:
        group = symbol.split("=")[1]
        symbols = su.read_json(f"../config/symbols-group{group}.json")
    else:
        symbols = symbol.split(",")
    cs = argv[1]

    config = su.read_json("config.json")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            TimedRotatingFileHandler(f"logs/EnrichSymbolData_G{'' if not group else group}_{cs}.log",
                                     when="h",
                                     interval=4,
                                     backupCount=42),
            logging.StreamHandler(sys.stdout)
        ]
    )

    count = 1
    total = len(symbols)
    for s in symbols:
        day, end_cs = get_augmentation_period(s, cs)
        while day < end_cs:
            query = {"size": ((end_cs-day) / (3600*24) )*6 , "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": s}}], "minimum_should_match": 1}}, {
                "range": {"open_time": {"gte": f"{day}", "lte": f"{end_cs}", "format": "strict_date_optional_time"}}}]}}}
            results = su.es_search(f"symbols-{cs}", query)['hits']['hits']

            data = {}
            for d in results:
                doc = d['_source']
                data[d['_id']] = doc
                day = doc['open_time']

        logging.info(f"\n\n {s} #{count} of {total} -- {len(data)} Documents \n\n")
        count += 1

        q_closes, q_volumes, q_trades = get_closes(s, cs, day, 200)
        aug = {}
        for k in data:
            doc_cs = data[k]
            aug[k] = enrich(s, cs, data, doc_cs, q_closes, q_volumes, q_trades )
            q_closes.append( doc_cs['close'] )
            q_closes.popleft()
            q_volumes.append(doc_cs['q_volume'] )
            q_volumes.popleft()
            q_trades.append(doc_cs['trades'])
            q_trades.popleft()

        su.es_bulk_create(f"symbols-aug-{cs}", aug, partial=500 )

if __name__ == "__main__":
    main(sys.argv[1:])
