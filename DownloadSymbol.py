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

symbol = sys.argv[1]
klines = fetch_candles(symbol, ld, "1d", int(delta), end_time=end_time)
