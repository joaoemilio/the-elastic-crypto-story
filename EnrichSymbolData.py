import logging
from logging.handlers import TimedRotatingFileHandler
import ScientistUtils as su
import ElasticSearchUtils as eu
from collections import deque
import time
import sys


def get_symbols(symbol):
    group = None
    if symbol == "ALL":
        symbols = su.get_symbols()
    elif "GROUP" in symbol:
        group = symbol.split("=")[1]
        symbols = su.read_json(f"config/symbols-group{group}.json")
    else:
        symbols = symbol.split(",")
    return group, symbols

def initialize(group):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            TimedRotatingFileHandler(f"logs/EnrichSymbolData_G{'' if not group else group}.log",
                                     when="h",
                                     interval=4,
                                     backupCount=42),
            logging.StreamHandler(sys.stdout)
        ]
    )

def query_first_and_last_doc(symbol: str, iname: str, es="ml-demo"):
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
    start_cs, end_cs = query_first_and_last_doc( symbol, f"symbols-{cs}", "ml-demo")
    if not start_cs:
        start_cs = su.get_ts("20191201")
    if not end_cs:
        end_cs = time.time()

    logging.info(
        f"{symbol} downloaded start={su.get_iso_datetime(start_cs)} end={su.get_iso_datetime(end_cs)}")

    if eu.es.indices.exists( f"symbols-aug-{cs}"):
        start_aug, end_aug = query_first_and_last_doc( symbol, f"symbols-aug-{cs}", "ml-demo")
        if not end_aug:
            day = start_cs
        else:
            day = end_aug
    else:
        day = start_cs

    print(f"AUGMENT {symbol} FROM start={su.get_iso_datetime(day)} TO end={su.get_iso_datetime(end_cs)}")
    return day, end_cs

def get_closes1(symbol, cs, ts_start, window_size):
    periods = {"5m": 60*5,  "15m": 60*15,
               "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60}
    ts_window_size = ts_start-window_size*periods[cs]

    query = {"size": window_size, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {
        "range": {"open_time": {"gte": f"{ts_window_size}", "lte": f"{ts_start}", "format": "strict_date_optional_time"}}}]}}}
    data = eu.es_search(f"symbols-{cs}", query)['hits']['hits']

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

def enrich(symbol, cs, data, doc_cs, dataws):
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

    doc_aug["dp"] = su.dp(doc_cs['close'], closes[-1])
    #print(f"close_1m={doc_1m['close']} close_cs={doc_cs['close']} dp={doc_cs['dp']} ")
    doc_aug['d0'] = su.delta(doc_cs['open'], doc_cs['close'])
    if len(volumes) > 0 and len(trades) > 0:
        doc_aug['q_volume_d0'] = su.delta(volumes[-1], q_vol_cs)
        doc_aug['trades_d0'] = su.delta(trades[-1], trades_cs)
    else:
        doc_aug['q_volume_d0'] = 0
        doc_aug['trades_d0'] = 0

    # future prices => low, high, close <==> 5m | 15m | 30m | 1h | 2h | 4h | 8h | 12h | 24h
    prices = {"5m": 60*5,  "15m": 60*15,  "30m": 60*30, "1h": 60*60, "2h": 2 *
                60*60, "4h": 4*60*60, "8h": 8*60*60, "12h": 12*60*60, "24h": 24*60*60, 
                "48h": 48*60*60, "72h": 72*60*60, "96h": 96*60*60}
    for p in prices:
        if cs == "1d":
            id_p = f"{symbol}_{su.get_yyyymmdd(doc_cs['open_time']+prices[p])}_{cs}"
        else:
            id_p = f"{symbol}_{su.get_yyyymmdd_hhmm(doc_cs['open_time']+prices[p])}_{cs}"
        if id_p in data:
            doc_p = data[id_p]
            if "future" not in doc_cs:
                doc_aug["future"] = {}
            doc_aug["future"][p] = {
                "low":   {"p": doc_p["low"], "d": su.delta(doc_cs['close'], doc_p["low"]) },
                "close": {"p": doc_p["close"], "d": su.delta(doc_cs['close'], doc_p["close"])},
                "high":  {"p": doc_p["high"], "d": su.delta(doc_cs['close'], doc_p["high"])}
            }
            for i in [5,10,15,20,25,30,50,100,150,200]:
                if doc_aug["future"][p]['high']['d'] >= i/1000:
                    doc_aug["future"][p]['buy'] = 1
                else:
                    doc_aug["future"][p]['buy'] = 0

    doc_aug["version"] = "1.0.0"

    return doc_aug

def enrich_cs(s, cs):
    day, end_cs = get_augmentation_period(s, cs)
    data = {}
    window_size = 200
    dataws = get_last(s, cs, day, window_size=window_size)
    while day < end_cs:
        query = {"size": 1000 , "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": s}}], "minimum_should_match": 1}}, {
            "range": {"open_time": {"gte": f"{day}", "lte": f"{end_cs}", "format": "strict_date_optional_time"}}}]}}}
        results = eu.es_search(f"symbols-{cs}", query)['hits']['hits']

        data = {}
        for d in results:
            doc = d['_source']
            data[d['_id']] = doc
            last_ot = doc['open_time']

        aug = {}
        for k in data:
            doc_cs = data[k]
            aug[k] = enrich(s, cs, data, doc_cs, dataws )
            if len(dataws) > window_size:
                k0 = None
                for kws in dataws:
                    k0 = kws
                    break
                if k0: del dataws[k0]
            dataws[k] = doc_cs

        eu.es_bulk_create(f"symbols-aug-{cs}", aug, partial=500 )

        day = last_ot
        logging.info(f"Continue {s} {cs} from {su.get_iso_datetime(day)}")

def enrich_symbol(s, cs):
    if not cs:
        enrich_cs(s, "1d")
        enrich_cs(s, "4h")
        enrich_cs(s, "1h")
        enrich_cs(s, "15m")
        enrich_cs(s, "5m")
    else:
        enrich_cs(s, cs)

def enrich_symbols(symbols, cs):
    count = 1
    total = len(symbols)
    for s in symbols:
        logging.info(f"Enriching symbol {s} #{count} of {total}")
        enrich_symbol(s, cs)
        count += 1

def main(argv):
    symbol = argv[0]
    cs = None
    if len(argv) > 1:
        cs = argv[1]
    group, symbols = get_symbols(symbol)
    initialize(group)
    enrich_symbols( symbols, cs )

if __name__ == "__main__":
    main(sys.argv[1:])
