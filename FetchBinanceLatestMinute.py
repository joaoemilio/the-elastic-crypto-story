from logging import error
import logging, time, sys
import TECSUtils as su

def download_from_binance(symbol):

    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=2"

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

r = download_from_binance( sys.argv[1] )
print(r[0])