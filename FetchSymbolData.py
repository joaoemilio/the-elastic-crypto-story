from EnrichSymbolData import enrich_cs, get_last
from logging import error, warn, info
import logging
from logging.handlers import TimedRotatingFileHandler
from os import strerror
import time
import ScientistUtils as su
import ElasticSearchUtils as eu
import sys
from collections import deque

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

def fetch1d( symbol ):
    data = {}

    fd, ld = query_first_and_last_doc(symbol, "symbols-1d")
    if not ld: 
        ld = su.get_ts("20191130")
    else:
        ld = ld + 24*3600
    if not fd: fd = su.get_ts("20191130")
    end_time = int(time.time())
    delta = (end_time - ld)/3600
    print(f"first day={su.get_yyyymmdd(fd)} last day={su.get_yyyymmdd(ld)} end_time={su.get_yyyymmdd(end_time)} delta={delta} {ld} {end_time}")
    if delta < 24: return # Não tentar fazer download do dia de hoje que está com candle aberto
    end_time = end_time-(24*3600)
    print(f"last download={su.get_yyyymmdd(ld)} {delta} periods end_time={su.get_yyyymmdd(end_time)}")
    klines = fetch_candles(symbol, ld, "1d", int(delta), end_time=end_time)
    for kline in klines:
        ot = su.get_yyyymmdd_hhmm(kline['open_time'])
        _id = f"{symbol}_{ot}_1d"
        kline["cs"] = "1d"
        data[_id] = kline
      
    return data

def fetch1m(symbol, ts_start, ts_end):

    # Only download until 'yesterday'
    now_ts = su.get_ts(su.get_yyyymmdd(time.time()))
    today_ts = su.get_ts(su.get_yyyymmdd(ts_start))    
    if now_ts == today_ts: return

    _f, day = query_first_and_last_doc(symbol, f"symbols-1m")

    query = {"size": 24*60, "query": {"bool":{"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}],"minimum_should_match": 1}},{"range": {"open_time": {"gte": f"{day}","lte": f"{ts_end}" ,"format": "strict_date_optional_time"}}}]}},"fields": ["id"], "_source": False}
    ids = []
    if eu.es.indices.exists( f"symbols-1m"):
        results = eu.es_search("symbols-1m", query)
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
                #if not eu.es_exists("symbols-1m", _id):
                o["cs"] = "1m"
                data[_id] = o

        su.log(f'End downloading day {su.get_yyyymmdd(day)} for 1m', 'fetch_candle')
            
        day += 3600*24
    return data

def fetch(symbol:str, cs:str, ts_start, ts_end):
    log(f"Lets fetch {symbol} cs={cs}")
    data = {}

    periods = int((24*3600)/candle_sizes[cs])
    r = fetch_candles(symbol, ts_start, cs, periods)
    for o in r:
        ot = su.get_yyyymmdd_hhmm(o['open_time'])
        _id = f"{symbol}_{ot}_{cs}"
        o["cs"] = cs
        data[_id] = o

    return data

def query_first_and_last_doc(symbol: str, iname: str):
    _first = {"size": 1, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": "1569342906", "lte": f"{time.time()}", "format": "strict_date_optional_time"}}}]}}, "fields": ["open_time"], "_source": False}

    _last = {"size": 1, "sort": [{"open_time": {"order": "desc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": "1569342906", "lte": f"{time.time()}", "format": "strict_date_optional_time"}}}]}}, "fields": ["open_time"], "_source": False}

    if not eu.es.indices.exists("symbols-1d"): return None, None

    fd = eu.es_search(iname, _first)
    ld = eu.es_search(iname, _last)

    fot = None
    lot = None
    if 'hits' in fd and 'hits' in fd['hits'] and len(fd['hits']['hits']) > 0:
        fot = int(fd['hits']['hits'][0]['fields']['open_time'][0])
    if 'hits' in ld and 'hits' in ld['hits'] and len(ld['hits']['hits']) > 0:
        lot = int(ld['hits']['hits'][0]['fields']['open_time'][0])

    return fot, lot

def main(argv):

    group, symbols = su.get_symbols(argv[0])

    if len(argv) > 1:
        cs = argv[1]
    else:
        cs = None

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            TimedRotatingFileHandler(f"logs/FetchSymbolData_G{'' if not group else group}.log",
                                        when="d",
                                        interval=1,
                                        backupCount=7),
            logging.StreamHandler(sys.stdout)
        ]
    )

    count = 1
    for symbol in symbols:
        data1d = fetch1d( symbol )
        if data1d:
            for key in data1d:
                day = data1d[key]['open_time']
                data5m = fetch( symbol, "5m", day, day+24*3600 )
                data15m = fetch( symbol, "15m", day, day+24*3600 )
                data1h = fetch( symbol, "1h", day, day+24*3600 )
                data4h = fetch( symbol, "4h", day, day+24*3600 )

            eu.es_bulk_create("symbols-1d", data1d, partial=1000)
            eu.es_bulk_create("symbols-15m", data15m, partial=1000)
            eu.es_bulk_create("symbols-1h", data1h, partial=1000)
            eu.es_bulk_create("symbols-4h", data4h, partial=1000)
            eu.es_bulk_create("symbols-5m", data5m, partial=1000)

        enrich_cs(symbol, "1d")
        enrich_cs(symbol, "4h")
        enrich_cs(symbol, "1h")
        enrich_cs(symbol, "15m")
        enrich_cs(symbol, "5m")

        count += 1

if __name__ == "__main__":
   main(sys.argv[1:])




'''
pegar  


'''