from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import time
import ScientistUtils as su
import ElasticSearchUtils as eu
from colorama import Style, Fore, Back

def download_klines(symbol, cs, start_time, window_size):
    # end_time in milliseconds
    periods = {"5m": 60*5,  "15m": 60*15,
               "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60}
    ts_window_size = start_time-window_size*periods[cs]

    # prepare empty result
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={cs}&startTime={ts_window_size*1000}&limit={window_size}"
    lines = None
    for i in (1,2,3):
        try:
            logging.info(f"trying binance call #{i}")
            lines = su.call_binance(url)
            break
        except ConnectionError as ce:
            logging.error(f"ConnectionError {ce.strerror}")
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

def fetch(symbol:str, cs:str, ts_start, window_size):
    logging.info(f"Lets fetch {s} cs={cs}")
    dataws = {}

    r = download_klines(s, cs, ts_start, window_size)
    doc_cs = None
    total = len(r)
    count = 0
    for o in r:
        count += 1
        ot = su.get_yyyymmdd_hhmm(o['open_time'])
        _id = f"{symbol}_{ot}_{cs}"
        o["cs"] = cs
        if count < total:
            dataws[_id] = o
        else:
            doc_cs = o

    return doc_cs, dataws


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


def get_last(symbol, cs, ts_start, window_size):
    periods = {"5m": 60*5,  "15m": 60*15,
               "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60}
    ts_window_size = ts_start-window_size*periods[cs]

    query = {"size": window_size, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {
        "range": {"open_time": {"gte": f"{ts_window_size}", "lte": f"{ts_start}", "format": "strict_date_optional_time"}}}]}}}
    results = eu.es_search(f"symbols-{cs}", query)
    dataws = {}
    if 'hits' in results and 'hits' in results['hits']:
        r = results['hits']['hits']        
        for d in r:
            dataws[d['_id']] = d['_source']

    return dataws


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        TimedRotatingFileHandler(f"logs/BuyNow.log",
                                    when="d",
                                    interval=1,
                                    backupCount=7),
        logging.StreamHandler(sys.stdout)
    ]
)

# Arguments
symbol = sys.argv[1]
cs = "1h"
iname = "buy-symbol-1h"

group, symbols = su.get_symbols(symbol)
print(symbols)
for s in symbols:
    # Download
    start_ts = su.get_ts_yyyymmdd_hh00(su.get_yyyymmdd_hh00(datetime.now().timestamp()))
    start_ts = start_ts-3600

    window_size = 201
    doc_cs, dataws = fetch( s, cs, start_ts, window_size )

    past = enrich_past(s, cs, doc_cs, dataws )
    aug = past if past else doc_cs
    aug = enrich_present(s, cs, aug, dataws )

    # Run Pipeline
    _id = f"{s}_{su.get_yyyymmdd_hhmm(start_ts)}_esquisito"
    if not eu.es_exists(iname, _id):
        print("asdf")
        su.log("antes")
        res = eu.es_create(iname, _id, aug, pipeline="pipeline-bnb-1hbuy10" )
        su.log("depois")
    else:
        print("3")
    doc = eu.es_get(iname, _id)['_source']
    # print(f"Buy {s} 1%={doc['ml']['inference']['future.1h.buy10_prediction']} ")
    # print(f"Buy {s} 5%={doc['ml5']['future.1h.buy50_prediction']}")
    inf1 = doc['ml']['inference']
    inf5 = doc['ml5']
    buy1 = inf1['future.1h.buy10_prediction']
    buy1prob = inf1['top_classes'][0]['class_probability']
    buy5 = inf5['future.1h.buy50_prediction']
    buy5prob = inf5['top_classes'][0]['class_probability']
    if buy1 == 1:
        print(f"Buy {s} {Fore.YELLOW if buy1 else Fore.WHITE}1%={buy1} {Style.RESET_ALL}name={inf1['top_classes'][0]['class_name']} probability={buy1prob:1.2f}")
    if buy5 == 1:
        print(f"Buy {s} {Fore.YELLOW if buy1 else Fore.WHITE}5%={buy5} {Style.RESET_ALL} name={inf5['top_classes'][0]['class_name']} probability={buy5prob:1.2f}")

    # Decide to buy or not