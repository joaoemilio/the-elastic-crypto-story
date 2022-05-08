
from logging import error
import logging
import time
import TECSUtils as su

#candle_sizes = {'1m':60, '5m': 5*60, '15m': 15*60, '1h': 3600, '4h': 4*3600, '1d': 24*3600}
candle_sizes = {'1m':60, '3m': 3*60, '5m': 5*60, '15m': 15*60, '30m': 30*60, '1h': 3600, '2h': 2*3600,  '4h': 4*3600, '6h': 6*3600, '8h': 8*3600, '12h': 12*3600, '1d': 24*3600}


def download_all_you_can(symbol:str, cs:str, ts_start, ts_end=None):
    log(f"Lets fetch {symbol} cs={cs}")
    data = {}

    periods = int((24*3600)/candle_sizes[cs])
    r = download_candles(symbol, ts_start, cs, periods, end_time=ts_end)
    for o in r:
        ot = su.get_yyyymmdd_hhmm(o['open_time'])
        _id = f"{symbol}_{ot}_{cs}"
        o["cs"] = cs
        data[_id] = o

    return data

def download(symbol:str, cs:str, ts_start, ts_end=None):
    log(f"Lets fetch {symbol} cs={cs}")
    data = {}

    periods = int((24*3600)/candle_sizes[cs])
    r = download_candles(symbol, ts_start, cs, periods, end_time=ts_end)
    for o in r:
        ot = su.get_yyyymmdd_hhmm(o['open_time'])
        _id = f"{symbol}_{ot}_{cs}"
        o["cs"] = cs
        data[_id] = o

    return data

def download_candles(symbol, day, cs, periods, end_time=None):
    # cs cannot be 1m
    if not end_time:
        end_time = day + 24*3600
    results = download_from_binance(symbol, cs, periods, end_time*1000 - 1)

    return results

def download_from_binance(symbol, cs, periods, end_time):
    # end_time in milliseconds

    # prepare empty result
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={cs}&limit={periods}&endTime={end_time}"
    debug(url)
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

def log(info):
    su.log(info)

def debug(msg):
    su.debug(msg)