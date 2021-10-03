import numpy as np
import json 

import logging
import requests
from os import listdir
from os.path import isfile, join
from datetime import datetime, timezone
import time
import pickle
import dateutil.parser as dtparser
from binance.client import Client 

def write_json(obj, fname):
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)

def write_csv(obj, fname):
    ''':param obj: must be an array of arrays representing a table'''

    with open(fname, 'w') as outfile:
        for line in obj:
            outfile.write("\t".join(str(c) for c in line) + "\n")
        
def read_json(fname):
    with open(fname, 'r') as json_file:
        return json.load(json_file)

def get_symbols():
    # Seleted symbols with q_volume_mm7 > 10e6 on 20210615, excluding BLVT and stable coins
    return read_json("config/symbols.json")

def write_object(obj, fname):
    with open(fname, 'wb') as output:  # Overwrites any existing file.
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)

def read_object(fname):
    with open(fname, 'rb') as input:
        return pickle.load(input)

def get_config():
    return read_json( f"config/config.json" )

def get_symbols(symbol):
    group = None
    if symbol == "ALL":
        symbols = read_json("config/symbols.json")
    elif "GROUP" in symbol:
        group = symbol.split("=")[1]
        symbols = read_json(f"config/symbols-group{group}.json")
    else:
        symbols = symbol.split(",")
    return group, symbols

start = time.time()
last = start

def print_timer(fn=''):
    global last
    now = time.time()
    logging.info(f'TIMER {fn} total={(now - start):.2f} delta={(now - last):.2f}')
    last = now

def log(msg, fn=''):
    global last
    now = time.time()
    logging.info(f'LOG {fn} {msg} total={(now - start):.2f} delta={(now - last):.2f}')
    last = now

total_count = None
interval_log = None
log_count = None
def start_progress(_total_count, _interval_log=1):
    global total_count, interval_log, log_count
    total_count = _total_count
    interval_log = _interval_log
    log_count = 0
    
def log_progress(current_count):
    if not interval_log or not total_count:
        print('ERROR in log_progress, call start_progress first')
        return 
    global log_count
    progress = current_count/total_count
    if  progress > log_count*interval_log/100:
        log_count += 1
        print(f'\tProgress: {int(progress*100)}%', end='\r')


def call_binance(url):
    payload={}
    headers = {
    'Content-Type': 'application/json'
    }
    
    '''
    A 429 will be returned when either rate limit is violated
    A 418 is an IP ban from 2 minutes to 3 days
    '''
    backoff = 30
    status_code = -1
    response = None
    while status_code == -1 or status_code == 429:
        response = requests.request("GET", url, headers=headers, data=payload) # response: status_code:int, json:method, text:str
        status_code = response.status_code
        if status_code == 429:
            if backoff > 100:
                raise BaseException("ERROR: Too many HTTP 429 responses")
            logging.warn(f'WARN: Sleeping {backoff}s due to HTTP 429 response')
            time.sleep(backoff)
            backoff = 2*backoff

    if status_code != 200:
        err = f"ERROR: HTTP {status_code} response for {url}"
        if status_code == 418:
            err = "ERROR: Binance API has banned this current IP"    
        logging.error(err)
        raise BaseException(err)
    return response.json()   

def get_yyyymmdd(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y%m%d")

def get_yyyymmdd_hhmm(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y%m%d_%H%M")

def get_iso_datetime(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M")
    
def get_ts(yyyymmdd):
    yyyymmdd = f'{yyyymmdd}'
    t = f'{yyyymmdd[0:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}T00:00:00+00:00'
    return int(datetime.timestamp(dtparser.isoparse(t)))

def get_ts2(tiso:str):
    # tiso must be like 2021-06-25T23:53
    tiso = tiso + ":00+00:00"
    return int(datetime.timestamp(dtparser.isoparse(tiso)))

def get_iso_datetime_sec(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

candle_sizes = {'5m': 5*60, '15m': 15*60, '1h': 3600, '4h': 4*3600, '1d': 24*3600}
candle_sizes1m = {'1m': 60, '5m': 5*60, '15m': 15*60, '1h': 3600, '4h': 4*3600, '1d': 24*3600}
candle_sizes1h = {'1h': 3600, '4h': 4*3600, '1d': 24*3600}

def klines_last_closed( symbol: str, limit=25, interval='4h' ):
    '''
    limit is the number of candles since start_time
    '''
    period = candle_sizes[interval]
    client = Client()
    tnow = time.time()
    # print(get_iso_datetime(tnow))
    # start_time of the current opened candle
    t = int(tnow/period)*period
    # print(get_iso_datetime(t))
    st = t - limit*period
    # print(get_iso_datetime(st))
    lines = client.get_klines(symbol=symbol, interval=interval, limit=limit, startTime=st*1000)

    lines_obj = []
    for l in lines:
        obj = {
                "open": float(l[1]), "high": float(l[2]), "low": float(l[3]), 
                "close": float(l[4]), "q_volume": float(l[7]), "trades": float(l[8]), "open_time": int(l[0]/1000)
            }
        lines_obj.append(obj)
    # print(f"count={len(lines)} first={get_iso_datetime(lines_obj[0]['open_time'])} last={get_iso_datetime(lines_obj[-1]['open_time'])}")

    return lines_obj

def klines_closed_since( symbol: str, start_time=None, interval='4h' ):
    '''
    limit is the number of candles since start_time
    '''
    period = candle_sizes[interval]
    client = Client()
    tnow = time.time()
    # print(get_iso_datetime(tnow))
    # start_time of last closed candle
    t = int(tnow/period)*period - period
    # print(get_iso_datetime(t))

    lines = client.get_klines(symbol=symbol, interval=interval, startTime=start_time*1000)
    lines_obj = []
    for l in lines:
        obj = {
                "open": float(l[1]), "high": float(l[2]), "low": float(l[3]), 
                "close": float(l[4]), "q_volume": float(l[7]), "trades": float(l[8]), "open_time": int(l[0]/1000)
            }
        if obj["open_time"] <= t:
            lines_obj.append(obj)
    # print(f"count={len(lines_obj)} first={get_iso_datetime(lines_obj[0]['open_time'])} last={get_iso_datetime(lines_obj[-1]['open_time'])}")

    return lines_obj

def adjust(v, w, sep=None):
    v = str(v)
    p = max(w - len(v), 0)
    r = " "*p + v
    if sep: r = r + sep
    return r

#############################
# Technical analisys indicators
#############################
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
