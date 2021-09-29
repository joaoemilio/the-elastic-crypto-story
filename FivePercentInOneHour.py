import sys
from elasticsearch import Elasticsearch
import json 
import time
from colorama import Fore, Back, Style
import ScientistUtils as su 
import ElasticSearchUtils as eu 

# Fields to be used with inference
'''
trades_d0       mid_bb99    bb5     d_vol_99    d0              close_mm5       std21
d_trades_200    mid_bb200   bb7     d_vol_200   dp              close_mm99      std25
d_trades_99     mid_bb5     bb51    d_vol_51    q_volume_d0     close_mm15      std51
d_trades_25     mid_bb10    bb99    d_vol_21    trades_d0       close_mm25      std20
                mid_bb51    bb10    d_vol_25                    close_mm200     std99
'''

def get_fields():
    fields = ["trades_d0", "d_trades_200", "d_trades_99", "d_trades_25", "mid_bb99", "mid_bb200", "mid_bb5", "mid_bb10", "mid_bb51", "bb5", "bb7", "bb51", "bb99", "bb10", "d_vol_99", "d_vol_200", "d_vol_51", "d_vol_21", "d_vol_25", "d0", "dp", "q_volume_d0", "trades_d0", "close_mm5", "close_mm99", "close_mm15", "close_mm25", "close_mm200", "std21", "std25", "std51", "std20", "std99", "close", "cs"]
    return fields

def read_json(fname):
    with open(fname, 'r') as json_file:
        return json.load(json_file)

config = read_json( f"config/config.json" )

es = Elasticsearch(    
    cloud_id= config["cloud_id"] ,
    http_auth=("elastic", config["cloud_password"])
)

# [ml-symbols-aug-1d-buy50-close-mm-d0-dp-std-analysis-1632908453887]

pipeline = "FivePercentInOneHour"

day = su.get_ts( sys.argv[1] )
end = su.get_ts( sys.argv[2] )
backtest = su.get_yyyymmdd_hhmm(time.time())
symbols = su.read_json(f"config/symbols.json")
fields = get_fields()
data = {}
count = 0

query = {"size": 10000 , "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"minimum_should_match": 1}}, {
    "range": {"open_time": {"gte": f"{day}", "lte": f"{end}", "format": "strict_date_optional_time"}}}]}}}
all = eu.es_search("symbols-aug-1d", query)['hits']['hits']

total = len(all)
su.start_progress(total)
data = {}
for row in all:
    count += 1
    su.log_progress(count)
    aug1d = row['_source']
    doc = {}
    _id = f"{row['_id']}_{backtest}"
    doc['symbol'] = aug1d['symbol']
    doc['open_time'] = aug1d['open_time']
    doc['open_time_iso'] = aug1d['open_time_iso']
    for f in fields:
        doc[f] = aug1d[f]
    doc['backtest_id'] = backtest
    data[ _id ] = doc

strategy_name = "five-percent-in-an-hour"
eu.es_bulk_create(strategy_name, data, pipeline="FivePercentInOneHour")
print(f"backtest id={backtest}")

