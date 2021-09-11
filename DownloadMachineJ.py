
from logging import warn
import os.path
import time
import ProphetUtils as pu 
from collections import deque
import sys
import numpy as np
import Calculations as ca

##################################################
# Data augmentation
##################################################

def data_augmentation(dcs, cs=None):
    '''
    :return: [[open_time, {symbol:{open, high, low, close, q_volume, trades, open_time, 
        d0_mm7, close_mm7, q_volume_mm7, trades_mm7}, ..}], ..]
    '''
    dcs2 = ca.transpose_dcs(dcs)
    for s in dcs2:
        candles = dcs2[s]
        ca.calculate_d0(candles)
        if cs != "1m":
            ca.calculate_dlow(candles)
            ca.calculate_mms(candles, 5)
            ca.calculate_mms(candles, 7)
            ca.calculate_mms(candles, 10)
            ca.calculate_mms(candles, 25)
            ca.calculate_bb(candles)
            ca.calculate_rsi(candles, 14)
            ca.calculate_rsi(candles, 6)

    return dcs

##################################################
# Download
##################################################

def is_laveraged(s):
    leveraged = ['UP', 'DOWN', 'BULL', 'BEAR']
    for lev in leveraged:
        if lev+"USDT" in s:
            return True
    return False 
  

candle_sizes = {'1m':60, '5m': 5*60, '15m': 15*60, '1h': 3600, '4h': 4*3600, '1d': 24*3600}

def download_klines(symbols, cs, periods, end_time):
    # end_time in milliseconds

    # prepare empty result
    results = [None]*periods
    pu.start_progress(len(symbols))
    for i, s in enumerate(symbols):
        pu.log_progress(i)
        url = f"https://api.binance.com/api/v3/klines?symbol={s}&interval={cs}&limit={periods}&endTime={end_time}"

        lines = pu.call_binance(url)
        '''
        [
            [
                1499040000000,      // Open time millis 0
                "0.01634790",       // Open 1
                "0.80000000",       // High 2
                "0.01575800",       // Low 3
                "0.01577100",       // Close 4
                "148976.11427815",  // Volume 5
                1499644799999,      // Close time millis 6
                "2434.19055334",    // Quote asset volume 7
                308,                // Number of trades 8
                "1756.87402397",    // Taker buy base asset volume 9
                "28.46694368",      // Taker buy quote asset volume 10
                "17928899.62484339" // Ignore.
            ]
        ]
        transform to:
        [open_time, [{symbol, open, high, low, close, q_volume, trades, d0}]]
        '''
        if len(lines) == 0: 
            continue
        elif len(lines) < periods:
            # NOTE missing data for a symbol is expected
            # returns the exact number of periods, repeating the open_time -> fill_the_gaps solve it
            log(f"ERROR lines={len(lines)} periods={periods}")
            times = [pu.get_iso_datetime(int(l[0]/1000))[-5:] for l in lines]
            log(f"DIAGNOSTIC s={s} times: {times}")
            # raise BaseException(f"ERROR lines={len(lines)} periods={periods}") 
            continue

        for i2, l in enumerate(lines):
            obj = {
                "open": float(l[1]), "high": float(l[2]), "low": float(l[3]), 
                "close": float(l[4]), "q_volume": float(l[7]), "trades": float(l[8]), "open_time": int(l[0]/1000)
            }

            if results[i2] == None:
                results[i2] = [obj["open_time"], {}]
            results[i2][1][s] = obj
    
    return results

def download_candles(symbols, day, cs, periods):
    # cs cannot be 1m
    end_time = day + 24*3600
    results = download_klines(symbols, cs, periods, end_time*1000 - 1)
    
    pu.log(f'End downloading day {pu.get_yyyymmdd(day)} for {cs}', 'download_candle')
    pu.write_json(results, raw_file(day, cs))
    return results

def download_candles1m(symbols, day):
    # prepare empty result

    end_time = day + 24*3600 # in seconds
    pairs = [ (440+25, end_time - 1000*60), (1000, end_time) ] # periods, end_time

    results_all = []
    for periods, end_time in pairs:
        results = download_klines(symbols, "1m", periods, end_time*1000-1)
        results_all += results

    pu.log(f'End downloading day {pu.get_yyyymmdd(day)} for 1m', 'download_candle')
    pu.write_json(results_all, raw_file(day, "1m"))
    return results_all


def log(info):
    pu.log(info)

##################################################
# 1d Logic
##################################################


def download1d( start ):
    '''
    if file does not existdownload data from 20191224 to yesterday
        save to data/raw/<date>-1d.json
    augment data and save to data/aug/<date>-1d.json
    generate ranking and save to data/ranking/<date>.json
    '''
    ts_end = int(time.time()/(24*3600))*24*3600
    ts_start = pu.get_ts( start )

    symbols = pu.get_symbols()
    day = ts_start
    dcs = []
    while day < ts_end:
        # download data from <start>
        fname = raw_file(day,"1d")
        if not os.path.exists(fname):
            pu.log(f'Will download day {pu.get_yyyymmdd(day)}', 'download_candles')
            download_candles(symbols, day, "1d", 1)
        dcs += pu.read_json(fname)
        day += 3600*24

    # augment data from 20210101 to yesterday
    dcs = data_augmentation(dcs)
    ts_start = pu.get_ts( start )
    for row in dcs:
        if row[0] < ts_start: continue
        fname = aug_file(row[0], "1d")
        pu.write_json([row], fname)

    # prepare v0b from 20210101 to yesterday
    for row in dcs:
        if row[0] < ts_start: continue
        # print(f"s_rankning empty: file={ranking_file(row[0] + 24*3600) }") # DEBUG
        
def raw_file(day,cs):
    day = pu.get_yyyymmdd(day)
    return f"j/data/raw/{day}-{cs}.json"

def aug_file(day,cs):
    day = pu.get_yyyymmdd(day)
    return f"j/data/aug/{day}-{cs}.json"

def ranking_file(day):
    day = pu.get_yyyymmdd(day)
    return f"j/data/ranking/{day}.json"

##################################################
# Logic for 4h, 1h, ...
##################################################

def iso(t):
    return pu.get_iso_datetime(t)

def fill_the_gaps(dcs, cs, fname, day):

    for i, r in enumerate(dcs):

        # EXCEPTION 4
        if r == None:
            log(f"EXCEPTION_4 i={i} day={iso(day)} fname={fname} dcs={dcs}")
            break

        # EXCEPTION 2
        te = day - 25*candle_sizes[cs] + i*candle_sizes[cs] # expected time for a row 
        t0 = r[0]
        if te != t0:
            r[0] = te
        
        # EXCEPTION 3
        for s in r[1]:
            ts = r[1][s]["open_time"]
            if  ts != te:
                r[1][s]["open_time"] = te
                r[1][s]["close"] = r[1][s]["open"]

    r1 = None
    for r in dcs:
        if r1:
            for s in r1[1]:
                if  r1[1][s]["close"] == r1[1][s]["open"] and s in r[1]:
                    r1[1][s]["close"] = r[1][s]["open"]
                    r1[1][s]["high"] = max(r1[1][s]["open"], r1[1][s]["close"], r1[1][s]["high"])
                    r1[1][s]["low"] = min(r1[1][s]["open"], r1[1][s]["close"], r1[1][s]["low"])
        r1 = r

    return dcs


def debug_gaps(dcs, cs, fname, day, symbols):
    '''
    IDEAL:
    i1, t1, SA:t1, SB:t1, SC:t1
    i2, t2, SA:t2, SB:t2, SC:t2

    EXCEPTION 1:
    i1, t1, SA:t1,      , SC:t1
    i2, t2, SA:t2,      , SC:t2

    EXCEPTION 2:
    i1, t1, SA:t1, SB:t1, SC:t1
    i2, t1, SA:t1, SB:t2, SC:t2

    EXCEPTION 3:
    i1, t1, SA:t1, SB:t1, SC:t1
    i2, t2, SA:t2, SB:t1, SC:t2

    EXCEPTION 4:
    i1 None
    i2 None

    t starts/ends where it shoould be?
    the number of t is exaclty what should be?
    is there any gaps between t?
    is there any t repetition?
    open_time is always equal to t?

    '''

    exceptions = {"EXCEPTION_1":0,"EXCEPTION_2":0,"EXCEPTION_3":0,"EXCEPTION_4":0}
    for i, r in enumerate(dcs):

        # EXCEPTION 4
        if r == None:
            log(f"EXCEPTION_4 i={i} day={iso(day)} fname={fname} dcs={dcs}")
            exceptions["EXCEPTION_4"] += 1
            break
        
        # EXCEPTION 1
        found = set()
        for s in r[1]:
            found.add(s)
        if len(found) != len(symbols):
            missing = set(symbols) - found
            log(f"EXCEPTION_1  fname={fname} missing={missing}")
            exceptions["EXCEPTION_1"] += 1

        # EXCEPTION 2
        te = day - 25*candle_sizes[cs] + i*candle_sizes[cs] # expected time for a row 
        t0 = r[0]
        if te != t0:
            log(f"EXCEPTION_2 i={i} te={iso(te)} t0={iso(t0)} fname={fname}")
            exceptions["EXCEPTION_2"] += 1
        
        # EXCEPTION 3
        for s in r[1]:
            ts = r[1][s]["open_time"]
            if  ts != te:
                log(f"EXCEPTION_3 i={i} te={iso(te)} ts={iso(ts)} fname={fname}")
                exceptions["EXCEPTION_3"] += 1

    if exceptions["EXCEPTION_3"] > 0 or exceptions["EXCEPTION_2"] > 0 or exceptions["EXCEPTION_4"] > 0 or exceptions["EXCEPTION_1"] > 0:
        log(f"---- EXCEPTIONS fname={fname} exceptions={exceptions}")


def download(cs:str, start:str, reaugment:bool):
    '''
    considers only symbols from get_ranking
    if file does not exist download data from 20200101 to yesterday
        save to data/raw/<date>-<cs>.json
    augment data and save to data/aug/<date>-<cs>.json
    '''    
    ts_end = int(time.time()/(24*3600))*24*3600
    ts_start = pu.get_ts( start )+24*3600

    log(f"Lets download/augment cs={cs}")
    day = ts_start
    pu.start_progress(int( (ts_end - ts_start)/(24*3600) ))
    symbols = pu.get_symbols()
    while day < ts_end:
        pu.log_progress(int( (day - ts_start)/(24*3600) ))
                
        # download data if needed
        fname = raw_file(day,cs)
        if not os.path.exists(fname):
            pu.log(f'Will download {pu.get_yyyymmdd(day)} cs={cs}', 'download_candles')

            periods = int((24*3600)/candle_sizes[cs]) + 25
            download_candles(symbols, day, cs, periods)
            download_candles1m(symbols, day) # NOTE +25 periods is hard coded in the function
        
        fname2 = aug_file(day, cs)
        if not os.path.exists(fname2) or reaugment:
            # load data
            dcs = pu.read_json(fname)
            dcs = fill_the_gaps(dcs, cs, fname, day) 
            debug_gaps(dcs, cs, fname, day, symbols)
            
            # augment data
            dcs = data_augmentation(dcs, cs)
            
            # remove dcs rows outside the day
            n_dcs = []
            for r in dcs:
                if int(r[0]/(3600*24))*(3600*24) != day: continue
                n_dcs.append(r)

            # save the aug data
            pu.write_json(n_dcs, fname2)

        day += 3600*24
  

def main(argv):
    print('--------------------------------------------------------------------------------')
    print(f" python3 DownloadMachine2.py [reaugment] ")
    print('--------------------------------------------------------------------------------')

    start = argv[0]
    reaugment = len(argv) > 1

    download1d( start )
    download("4h", start, reaugment)
    download("1h", start, reaugment)
    download("15m", start, reaugment)
    download("5m", start, reaugment)
    download("1m", start, reaugment)
    print('\n\n\n')

if __name__ == "__main__":
   main(sys.argv[1:])


