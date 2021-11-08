'''
Esse código é focado em apenas 1 symbol/pair por vez e apenas 1 período (1m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1m) por vez
Identificar quando o symbol foi listado na Exchange (Binance/Coinbase)
Verificar qual foi o último registro baixado
Iniciar o processo de download a partir registro imediatamente após o último baixado
Determinar o timestamp atual para servir de ponto de referencia para o termino
Terminar o processo ao baixar o último candle fechado referente ao período em questão

python3 DonwloadSymbol.py <BTCUSDT> <1d|4h|1h|15m|5m|1m>

'''
import sys 
import time
import ScientistUtils as su

def download_candles(crypto, pair, cs, start_time):
    symbol = f"{crypto}{pair}"

    # prepare empty result
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={cs}&startTime={start_time}"
    print(url)
    lines = None
    for i in (1,2,3):
        try:
            lines = su.call_binance(url)
            break
        except ConnectionError as ce:
            print(f"ConnectionError {ce.strerror}")
            print("waiting 5 seconds to call binance again")
            time.sleep(5)
    
    return lines

crypto = sys.argv[1]
pair = sys.argv[2]
cs = sys.argv[3]

startTime = su.get_ts_yyyymmdd_hhmm("20211101_0000")*1000

klines = download_candles(crypto, pair, cs, startTime)
for k in klines:
    print( f"{crypto}{pair}-{cs}-{su.get_iso_datetime(k[0]/1000)} - {k}" )
    last_time = k[0]

klines = download_candles(crypto, pair, cs, last_time+3600)
for k in klines:
    print( f"{crypto}{pair}-{cs}-{su.get_iso_datetime(k[0]/1000)} - {k}" )
    last_time = k[0]
