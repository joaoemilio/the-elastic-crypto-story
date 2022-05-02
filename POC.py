import BinanceAPI as bapi
from BinanceAPI import Client
import TECSUtils as utils

# symbols = bapi.get_symbols()
# print(symbols)

# for s in symbols:
#     klines = bapi.client.get_historical_klines(s, Client.KLINE_INTERVAL_1DAY, "1 Jan, 2017")
#     start = utils.get_yyyymmdd( klines[0][0] / 1000 )
#     print(f'    "{s}" : {start},')

bucket = utils.get_bucket( utils.s3, "elastic-crypto-story")
res = utils.isfile_s3(bucket, "summary-total.txt")
print(res)