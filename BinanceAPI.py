import TECSUtils as su
from logging import error
import logging
import time
from binance.client import Client

client = Client()
def get_symbols():
    symbols = []
    list = client.get_exchange_info()  
    for l in list["symbols"]:
        asset = l['quoteAsset']
        if asset == "USDT":
            symbols.append(l['symbol'])

    return symbols

def get_first_kline(s,cs):
    return client.get_historical_klines(s, cs, "1 Jan, 2017")    

def download_candles(crypto, pair, cs, start_time):
    symbol = f"{crypto}{pair}"

    # prepare empty result
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={cs}"
    print(url)
    lines = None
    for i in (1,2,3):
        try:
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

