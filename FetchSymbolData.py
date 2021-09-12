
from logging import error, warn, info
from os import strerror
import os.path
import time
import ScientistUtils as su
from collections import deque
import sys
import numpy as np
import Calculations as ca

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
            info(f"trying binance call #{i}")
            lines = su.call_binance(url)
            break
        except ConnectionError as ce:
            error(f"ConnectionError {ce.strerror}")
            info("waiting 5 seconds to call binance again")
            time.sleep(5)


    if len(lines) < periods:
        # NOTE missing data for a symbol is expected
        # returns the exact number of periods, repeating the open_time -> fill_the_gaps solve it
        log(f"ERROR lines={len(lines)} periods={periods}")
        times = [su.get_iso_datetime(int(l[0]/1000))[-5:] for l in lines]
        log(f"DIAGNOSTIC s={symbol} times: {times}")
        return 

    results = []
    for i2, l in enumerate(lines):
        obj = {
            "symbol": symbol, "open": float(l[1]), "high": float(l[2]), "low": float(l[3]), 
            "close": float(l[4]), "q_volume": float(l[7]), "trades": float(l[8]), "open_time": int(l[0]/1000),
            "open_time_ms": int(l[0]), "open_time_iso": su.get_iso_datetime_sec( int(l[0])/1000 )
        }
        results.append(obj)
    
    return results

def fetch_candles(symbol, day, cs, periods):
    # cs cannot be 1m
    end_time = day + 24*3600
    results = download_klines(symbol, cs, periods, end_time*1000 - 1)

    return results

def log(info):
    su.log(info)

##################################################
# 1d Logic
##################################################

def fetch1d( symbol, start, end=None ):
    if not end:
        ts_end = int(time.time()/(24*3600))*24*3600
    else:
        ts_end = su.get_ts( end )

    ts_start = su.get_ts( start )

    day = ts_start
    while day < ts_end:
        print(f'\tprocessing {su.get_yyyymmdd(day)}', end='\r')
        # download data from <start>
        ot = su.get_yyyymmdd(day)
        _id = f"{symbol}_{ot}_1d"
        if not su.es_exists("symbols", _id):
            kline = fetch_candles(symbol, day, "1d", 1)
            if kline: 
                obj = kline[0]
                obj["cs"] = "1d"
                su.es_create( "symbols", _id, obj )
        
        day += 3600*24

##################################################
# Logic for 4h, 1h, ...
##################################################

def iso(t):
    return su.get_iso_datetime(t)

def fetch(symbol:str, cs:str, start:str, end=None):

    if not end:
        ts_end = int(time.time()/(24*3600))*24*3600
    else:
        ts_end = su.get_ts( end )

    ts_start = su.get_ts( start )+24*3600

    log(f"Lets fetch {symbol} cs={cs}")
    day = ts_start

    while day < ts_end:
        su.log(f'Will download {su.get_yyyymmdd(day)} cs={cs}', 'download_candles')

        periods = int((24*3600)/candle_sizes[cs])
        r = fetch_candles(symbol, day, cs, periods)
        data = {}
        if r:
            for o in r:
                ot = su.get_yyyymmdd_hhmm(o['open_time'])
                _id = f"{symbol}_{ot}_{cs}"
                if not su.es_exists("symbols", _id):
                    o["cs"] = cs
                    #su.es_create( "symbols", _id, o )
                    data[_id] = o
                else:
                    print(f"Doc {_id} already exists. Skiping")
        
            if len(data) > 0:
                print(f"Bulk upload {len(data)} docs")
                su.es_bulk_create("symbols", data, partial=500)

        day += 3600*24

def main(argv):
    print('--------------------------------------------------------------------------------')
    print(f" python3 FetchSymbolData.py BTCUSDT 20210801 [20210901] <-- only BTCUSDT from start to [end]")
    print(f" python3 FetchSymbolData.py ALL 20210801 [20210901]<-- ALL symbols in symbols.json from start to [end]")
    print('--------------------------------------------------------------------------------')

    if argv[0] == "ALL":
        symbols = su.read_json("symbols.json")
        start = argv[1]
        if len(argv) == 3:
            end = argv[2]
        else:
            end = None
        for symbol in symbols:
            info(f"start fetching data for {symbol}")
            fetch1d( symbol, start, end )
            fetch( symbol, "4h", start, end)
            fetch(symbol, "1h", start, end)
            fetch(symbol, "15m", start, end)
            fetch(symbol, "5m", start, end)
            print('\n\n\n')

    else:
        symbol = argv[0]
        start = argv[1]
        if len(argv) == 3:
            end = argv[2]
        else:
            end = None

        fetch1d( symbol, start, end )
        fetch( symbol, "4h", start, end)
        fetch(symbol, "1h", start, end)
        fetch(symbol, "15m", start, end)
        fetch(symbol, "5m", start, end)
        print('\n\n\n')

if __name__ == "__main__":
   main(sys.argv[1:])


