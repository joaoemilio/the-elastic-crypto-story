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
    print(url)
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
    logging.info(f"first day={su.get_yyyymmdd(fd)} last day={su.get_yyyymmdd(ld)} end_time={su.get_yyyymmdd(end_time)} delta={delta} {ld} {end_time}")
    if delta < 24: return # Não tentar fazer download do dia de hoje que está com candle aberto
    end_time = end_time-(24*3600)
    logging.info(f"last download={su.get_yyyymmdd(ld)} {delta} periods end_time={su.get_yyyymmdd(end_time)}")
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

def get_next_hours_15m(symbol, ts_start, hours):
    ts_next_hours = ts_start+hours*3600

    query = { "size": 10000, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": f"{ts_start}", "lte": f"{ts_next_hours}", "format": "strict_date_optional_time"}}}]}}, "fields": ["close", "high", "low", "open_time"], "_source": False}
    results = eu.es_search(f"symbols-15m", query)
    data = {}
    if 'hits' in results and 'hits' in results['hits']:
        r = results['hits']['hits']        
        for d in r:
            data[d['_id']] = { "open_time" : d['fields']['open_time'][0], "close": d['fields']['close'][0], "high": d['fields']['high'][0], "low": d['fields']['low'][0] }

    return data

def get_cs_documents(symbol, cs, ts_start, ts_next_hours):

    data = {}
    last_ot = None
    while ts_start < ts_next_hours:
        query = { "size": 1000, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": f"{ts_start}", "lte": f"{ts_next_hours}", "format": "strict_date_optional_time"}}}]}}, "fields": ["close", "high", "low", "open_time"], "_source": False}
        results = eu.es_search(f"symbols-{cs}", query)
        count = 0
        if 'hits' in results and 'hits' in results['hits']:
            r = results['hits']['hits']        
            for d in r:
                count += 1
                data[d['_id']] = { "open_time" : d['fields']['open_time'][0], "close": d['fields']['close'][0], "high": d['fields']['high'][0], "low": d['fields']['low'][0] }
                last_ot = d['fields']['open_time'][0]
        ts_start = last_ot
        if count < 1000: break

    return data

def enrich_past(symbol, cs, doc_cs, dataws):
    if len(dataws) == 0: return

    close_cs = doc_cs['close']
    q_vol_cs = doc_cs['q_volume']
    trades_cs = doc_cs['trades']
    mms = [5, 7, 9, 10, 15, 20, 21, 25, 51, 99, 200]
    closes = [dataws[d]['close'] for d in dataws]
    trades = [dataws[d]['trades'] for d in dataws]
    volumes = [dataws[d]['q_volume'] for d in dataws]
    doc_aug = doc_cs.copy()
    for mm in mms:
        if mm <= len(closes):
            doc_aug[f"close_mm{mm}"] = su.moving_avg(close_cs, closes[-mm:], mm)
            doc_aug[f"trades_mm{mm}"] = su.moving_avg(trades_cs, trades[-mm:], mm)
            doc_aug[f"q_volume_mm{mm}"] = su.moving_avg(q_vol_cs, volumes[-mm:], mm)
            doc_aug[f"std{mm}"] = su.std_dev(close_cs, closes[-mm:], mm)
            std = doc_aug[f"std{mm}"]
            closemm = doc_aug[f"close_mm{mm}"]
            tradesmm = doc_aug[f'trades_mm{mm}']
            doc_aug[f"bb{mm}"] = su.bb(close_cs, closemm , std )
            doc_aug[f'd_vol_{mm}'] = su.delta(doc_cs['q_volume'], doc_aug[f'q_volume_mm{mm}'])
            d_trades = su.delta(trades_cs, tradesmm)
            doc_aug[f'd_trades_{mm}'] = d_trades
            doc_aug[f"mid_bb{mm}"] = su.mean(close_cs, closes[-mm:], mm)

        else:
            doc_aug[f"close_mm{mm}"] = 0
            doc_aug[f"trades_mm{mm}"] = 0
            doc_aug[f"q_volume_mm{mm}"] = 0
            doc_aug[f"std{mm}"] = 0
            doc_aug[f"mid_bb{mm}"] = 0
            doc_aug[f"bb{mm}"] = 0
            doc_aug[f'd_vol_{mm}'] = 0
            doc_aug[f'd_trades_{mm}'] = 0

    doc_aug["version"] = "1.0.0"

    return doc_aug


def enrich_present(symbol, cs, doc_cs, dataws):

    q_vol_cs = doc_cs['q_volume']
    trades_cs = doc_cs['trades']
    closes = [dataws[d]['close'] for d in dataws]
    trades = [dataws[d]['trades'] for d in dataws]
    volumes = [dataws[d]['q_volume'] for d in dataws]
    doc_aug = doc_cs.copy()
    doc_aug["cs"] = cs

    if len(closes) > 0:
        doc_aug["dp"] = su.dp(doc_cs['close'], closes[-1])
    else:
        doc_aug["dp"] = 0
    #print(f"close_1m={doc_1m['close']} close_cs={doc_cs['close']} dp={doc_cs['dp']} ")
    doc_aug['d0'] = su.delta(doc_cs['open'], doc_cs['close'])
    if len(volumes) > 0 and len(trades) > 0:
        doc_aug['q_volume_d0'] = su.delta(volumes[-1], q_vol_cs)
        doc_aug['trades_d0'] = su.delta(trades[-1], trades_cs)
    else:
        doc_aug['q_volume_d0'] = 0
        doc_aug['trades_d0'] = 0

    return doc_aug

def enrich_future(symbol, cs, data, doc_cs, dataws):

    close_cs = doc_cs['close']
    ot = doc_cs["open_time"]
    doc_aug = doc_cs.copy()

    # future prices => low, high, close <==> 5m | 15m | 30m | 1h | 2h | 4h | 8h | 12h | 24h
    prices = {"15m": 60*15,  "30m": 60*30, "1h": 60*60, "2h": 2 *
                60*60, "4h": 4*60*60, "8h": 8*60*60, "12h": 12*60*60, "24h": 24*60*60, 
                "48h": 48*60*60, "72h": 72*60*60, "96h": 96*60*60}
    for p in prices:
        id_p = f"{symbol}_{su.get_yyyymmdd_hhmm(doc_cs['open_time']+24*3600+prices[p])}_15m"
        if id_p in data:
            doc_p = data[id_p]
            if "future" not in doc_aug:
                doc_aug["future"] = {}
            doc_aug["future"][p] = {
                "low":   {"p": doc_p["low"], "d": su.delta(doc_cs['close'], doc_p["low"]) },
                "close": {"p": doc_p["close"], "d": su.delta(doc_cs['close'], doc_p["close"])},
                "high":  {"p": doc_p["high"], "d": su.delta(doc_cs['close'], doc_p["high"])}
            }
            for i in [5,10,15,20,25,30,50,100,150,200]:
                ts_final = ot + prices[p]
                t = ot
                while t <= ts_final:
                    id_15m = f"{symbol}_{su.get_yyyymmdd_hhmm(t)}_15m"
                    if id_15m in data:
                        doc_15m = data[id_15m]
                        c15m = doc_15m['close']
                        if su.delta( close_cs, c15m ) >= i/1000:
                            doc_aug["future"][p][f'buy{i}'] = 1
                            break
                        else:
                            doc_aug["future"][p][f'buy{i}'] = 0
                    else:
                        doc_aug["future"][p][f'buy{i}'] = 0

                    t = t+60*15
        else:
            if "future" not in doc_aug:
                doc_aug["future"] = {}
            doc_aug["future"][p] = {
                "low":   {"p": 0, "d": 0},
                "close": {"p": 0, "d": 0},
                "high":  {"p": 0, "d": 0}
            }
            for i in [5,10,15,20,25,30,50,100,150,200]:
                doc_aug["future"][p][f'buy{i}'] = 0

    doc_aug["version"] = "1.0.0"

    return doc_aug

def get_last(symbol, cs, ts_start, window_size):
    periods = {"5m": 60*5,  "15m": 60*15,
               "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60}
    ts_window_size = ts_start-window_size*periods[cs]

    query = {"size": window_size, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {
        "range": {"open_time": {"gte": f"{ts_window_size}", "lte": f"{ts_start}", "format": "strict_date_optional_time"}}}]}}}
    results = eu.es_search(f"symbols-{cs}", query)
    data = {}
    if 'hits' in results and 'hits' in results['hits']:
        r = results['hits']['hits']        
        for d in r:
            data[d['_id']] = d['_source']

    return data

def enrich_cs(s, cs):
    day, end_cs = get_augmentation_period(s, cs)
    while day < end_cs:
        logging.info(f"Augmenting {s} {cs} from {su.get_yyyymmdd(day)} to {su.get_yyyymmdd(end_cs)}")
        data = {}
        window_size = 200
        dataws = get_last(s, cs, day, window_size=window_size)
        query = {"size": 100 , "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": s}}], "minimum_should_match": 1}}, {
            "range": {"open_time": {"gte": f"{day}", "lte": f"{end_cs}", "format": "strict_date_optional_time"}}}]}}}
        results = eu.es_search(f"symbols-{cs}", query)['hits']['hits']

        data = {}
        first_ot = 0
        last_ot = 0
        next_ot = 0
        for d in results:
            doc = d['_source']
            data[d['_id']] = doc
            if not first_ot: first_ot = doc['open_time']
            next_ot = doc['open_time'] #This is to make sure it keeps iterating every 1000 records (specially for older cryptos going back to 2017)

        periods = {"5m": 60*5,  "15m": 60*15, "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60}
        aug = {}
        #_next = get_next_hours_15m(s, aug_time, 96)
        last_ot = next_ot+96*3600
        print(f"{su.get_iso_datetime(first_ot)} last={su.get_iso_datetime(last_ot)}")
        _next = get_cs_documents( s, "15m", first_ot, last_ot )
        for k in data:
            doc_cs = data[k]
            print(f"Augment s={doc_cs['symbol']} t={su.get_iso_datetime(doc_cs['open_time'])} cs={cs}")
            aug_time = doc_cs['open_time'] + periods[cs]
            past = enrich_past(s, cs, doc_cs, dataws )
            aug[k] = past if past else doc_cs
            aug[k] = enrich_present(s, cs, aug[k], dataws )
            aug[k] = enrich_future(s, cs, _next, aug[k], dataws )
            if len(dataws) > window_size:
                k0 = None
                for kws in dataws:
                    k0 = kws
                    break
                if k0: del dataws[k0]
            dataws[k] = doc_cs

        eu.es_bulk_create(f"aug-symbols-{cs}", aug, partial=100 )
        day = next_ot

def query_first_and_last_doc(symbol: str, iname: str, es="prophet"):
    _first = {"size": 1, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": "1569342906", "lte": f"{time.time()}", "format": "strict_date_optional_time"}}}]}}, "fields": ["open_time"], "_source": False}

    _last = {"size": 1, "sort": [{"open_time": {"order": "desc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {"range": {"open_time": {"gte": "1569342906", "lte": f"{time.time()}", "format": "strict_date_optional_time"}}}]}}, "fields": ["open_time"], "_source": False}


    fd = eu.es_search(iname, _first, es)
    ld = eu.es_search(iname, _last, es)


    fot = None
    lot = None
    if 'hits' in fd and 'hits' in fd['hits'] and len(fd['hits']['hits']) > 0:
        fot = int(fd['hits']['hits'][0]['fields']['open_time'][0])
    if 'hits' in ld and 'hits' in ld['hits'] and len(ld['hits']['hits']) > 0:
        lot = int(ld['hits']['hits'][0]['fields']['open_time'][0])

    return fot, lot

def get_augmentation_period(symbol: str, cs: str):
    start_cs, end_cs = query_first_and_last_doc( symbol, f"symbols-{cs}", "prophet")
    if not start_cs:
        start_cs = su.get_ts("20191201")
    if not end_cs:
        end_cs = time.time()

    if eu.es.indices.exists( f"aug-symbols-{cs}"):
        start_aug, end_aug = query_first_and_last_doc( symbol, f"aug-symbols-{cs}", "prophet")
        if not end_aug:
            day = start_cs
        else:
            day = end_aug
    else:
        day = start_cs

    day = day - 96*3600 # recent days do not have fields covering the future, as it didn't exist yet. Now, there are 24 "new" hours to fill the gap of an augmented day 96 hours ago
    return day, end_cs

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
        logging.info(f"Fetching data for {symbol}")
        data1d = fetch1d( symbol )
        fd, ld = None, None
        if data1d:
            for key in data1d:
                day = data1d[key]['open_time']
                if not fd: fd = day
                data5m = fetch( symbol, "5m", day, day+24*3600 )
                data15m = fetch( symbol, "15m", day, day+24*3600 )
                data1h = fetch( symbol, "1h", day, day+24*3600 )
                data4h = fetch( symbol, "4h", day, day+24*3600 )

                eu.es_bulk_create("symbols-5m", data5m, partial=1000)
                eu.es_bulk_create("symbols-15m", data15m, partial=1000)
                eu.es_bulk_create("symbols-1h", data1h, partial=1000)
                eu.es_bulk_create("symbols-4h", data4h, partial=1000)
            eu.es_bulk_create("symbols-1d", data1d, partial=1000)

        enrich_cs(symbol, "1d")
        enrich_cs(symbol, "4h")
        enrich_cs(symbol, "1h")
        # enrich_cs(symbol, "15m")
        # enrich_cs(symbol, "5m")

        count += 1

if __name__ == "__main__":
   main(sys.argv[1:])




'''
pegar  


'''