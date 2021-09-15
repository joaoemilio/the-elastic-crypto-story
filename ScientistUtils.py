

import json 
from elasticsearch import helpers, Elasticsearch
import elasticsearch
import logging
import requests
import pathlib
from os import listdir
from os.path import isfile, join
from datetime import datetime, timezone
import time
import pickle
import dateutil.parser as dtparser
from binance.client import Client 
    # see https://python-binance.readthedocs.io/en/latest/general.html

channels = {
            "buys":"https://hooks.slack.com/services/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",
            "sells":"https://hooks.slack.com/services/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",
            "alerts":"https://hooks.slack.com/services/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",
            "market":"https://hooks.slack.com/services/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",
            "prophet":"https://hooks.slack.com/services/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",
            }
            
def notify(msg, code=None, channel="alerts"):
    url = channels[channel]
    b = {
        "type": "mkdwn",
        "text": msg,
    }
    if code: b["text"] = f"{msg} ```{code}```"
    try:
        requests.post(url, json=b)
    except requests.exceptions.SSLError as e:
        logging.error(f"Error posting to {url} e={e}") # use proper logging

############ ELASTICSEARCH ##############

def es_exists(iname, id):
    return es.exists( id=id, index=iname)

def es_create( iname, _id, obj ):
    es.create( id=_id, body=obj, index=iname)

def es_bulk_create(iname, data, partial=100):
    count = 1
    if not partial: partial = len(data)
    actions = []
    for k in data:
        action = { "_index": iname, "_source": data[k], "id": k, "_id": k }
        actions.append( action )
        if partial and count == partial:

            for i in (1,2,3):
                try:
                    helpers.bulk(client=es,actions=actions)
                    break
                except elasticsearch.exceptions.ConnectionTimeout as cte:
                    logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
                    logging.info( "waiting 10s before retry sending docs to elasticsearch")
                    time.sleep(10)
            actions = []
            count = 0
        else:
            count += 1

    helpers.bulk(client=es,actions=actions)        

############################### IO ############################

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
    return read_json("symbols.json")

def write_object(obj, fname):
    with open(fname, 'wb') as output:  # Overwrites any existing file.
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)

def read_object(fname):
    with open(fname, 'rb') as input:
        return pickle.load(input)

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
        logging.info('ERROR in log_progress, call start_progress first')
        return 
    global log_count
    progress = current_count/total_count
    if  progress > log_count*interval_log/100:
        log_count += 1
        logging.info(f'\tProgress: {int(progress*100)}%', end='\r')


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
    
def iso(t):
    return get_iso_datetime(t)

def get_iso_datetime_sec(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")
def iso_sec(t):
    return get_iso_datetime_sec(t)

def get_ts(yyyymmdd):
    yyyymmdd = f'{yyyymmdd}'
    t = f'{yyyymmdd[0:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}T00:00:00+00:00'
    return int(datetime.timestamp(dtparser.isoparse(t)))

def get_ts2(tiso:str):
    # tiso must be like 2021-06-25T23:53
    tiso = tiso + ":00+00:00"
    return int(datetime.timestamp(dtparser.isoparse(tiso)))

# get_yyyymmdd(get_ts(20210101))

# print(get_ts(20210101)) # DEBUG

def get_file_name(day, candle_size='5m'):
    today = int(time.time()/(24*3600))*24*3600    # today 0 GMT
    if day == today:
        candle_size += "-today"
    fname = f"data/{day}-{get_yyyymmdd(day)}-{candle_size}.json"
    return fname


candle_sizes = {'5m': 5*60, '15m': 15*60, '1h': 3600, '4h': 4*3600, '1d': 24*3600}
candle_sizes1m = {'1m': 60, '5m': 5*60, '15m': 15*60, '1h': 3600, '4h': 4*3600, '1d': 24*3600}
candle_sizes1h = {'1h': 3600, '4h': 4*3600, '1d': 24*3600}

def get_index(open_time, day):
    r = (open_time - day)/(60*5)
    return int(r)

def reflectionCall():
    print("called")


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


def dict2lines(ptrades, spaces=2, sep=None):
    # Calculates columns widths
    cols = None
    for t in ptrades:
        if not cols:
            cols = [0]*len(t)
        i = 0
        for k in t:
            cols[i] = max(cols[i], len(str(t[k]))+spaces, len(k)+spaces)
            i += 1

    # Format string lines 
    htrades = ""
    ftrades = []
    for t in ptrades:
        # create the header
        if len(htrades) == 0:
            i = 0
            for k in t:
                htrades += adjust(k, cols[i], sep)
                i += 1
            ftrades.append(htrades)
        
        # append the lines
        line = ""
        i = 0
        for k in t:
            line += adjust(t[k], cols[i], sep)
            i += 1
        ftrades.append(line)

    return ftrades

def adjust(v, w, sep=None):
    v = str(v)
    p = max(w - len(v), 0)
    r = " "*p + v
    if sep: r = r + sep
    return r


######################
# DiscipleUtils 
######################

def read_txt(fname):

    lines = []
    with open(fname, "r") as f:
        line = f.readline()
        while line:
            lines.append( line )
            line = f.readline()

    return "".join(lines)

def is_disciple_alive(customer):
    fname = f"logs/SlimDisciple4-{customer}.log"

    f = pathlib.Path(fname)
    if not f.exists(): return False

    tnow = time.time()
    tmod = f.stat().st_mtime
    return (tnow - tmod) < 70


def is_prophet_alive():
    fname = f"logs/SlimProphet4.log"

    f = pathlib.Path(fname)
    if not f.exists(): return False

    tnow = time.time()
    tmod = f.stat().st_mtime
    return (tnow - tmod) < 70



def get_gain_loss(v1, v2):
    return (v2 / v1 * 100) - 100   

def get_disciple(customer):
    '''
    @return customer/disciple object
    '''
    disciple = read_json( f"config/disciple/{customer}.json")
    return disciple

def save_disciple(customer, obj):
    write_json( obj, f"config/disciple/{customer}.json")
    return True


def add_customer_keys( customer, key, secret ):
    customers_keys = read_json("config/isengard.json")
    if customer not in customers_keys:
        customers_keys[customer] = { "api_key": key, "api_secret" : secret }
        write_json( customers_keys, f"config/isengard.json")
    else:
        logging.info(f"{customer} already have registered keys.")

def create_disciple(customer, obj, key, secret):
    write_json( obj, f"config/disciple/{customer}.json")
    add_customer_keys(customer=customer, key=key, secret=secret)
    return True

def get_disciples(enabled_only=False):
    disciples = [f.replace(".json", "") for f in listdir("config/disciple") if isfile(join("config/disciple", f)) and f.endswith(".json") ]

    if enabled_only:
        active_disciples = []
        for d in disciples:
            disciple = read_json( f"config/disciple/{d}.json")
            if disciple['enabled']:
                active_disciples.append(d)
        disciples = active_disciples

    return disciples

config = read_json( f"config.json" )
es = Elasticsearch(    
    cloud_id= config["cloud_id"] ,
    http_auth=("elastic", config["cloud_password"])
)
