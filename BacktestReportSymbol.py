import sys
from elasticsearch import Elasticsearch
import json 
import time
from colorama import Fore, Back, Style
import ScientistUtils as su 
import ElasticSearchUtils as eu 

#strategy_name = "five-percent-in-an-hour"
pipeline = "pipeline-bnb-1hbuy10"
strategy_name = "eth-five-percent-in-one-hour"
def get_fields():
    fields = ["trades_d0", "d_trades_200", "d_trades_99", "d_trades_25", "mid_bb99", "mid_bb200", "mid_bb5", "mid_bb10", "mid_bb51", "bb5", "bb7", "bb51", "bb99", "bb10", "d_vol_99", "d_vol_200", "d_vol_51", "d_vol_21", "d_vol_25", "d0", "dp", "q_volume_d0", "trades_d0", "close_mm5", "close_mm99", "close_mm15", "close_mm25", "close_mm200", "std21", "std25", "std51", "std20", "std99", "close", "cs"]
    return fields

def read_json(fname):
    with open(fname, 'r') as json_file:
        return json.load(json_file)

def comission(b, pd=None):
    if not pd:
        return b * 0.075/100
    else:
        _b = b * pd
        b = b+_b
        b = b - comission(b)
        return b

config = read_json( f"config/config.json" )

es = Elasticsearch(    
    cloud_id= config["cloud_id"] ,
    http_auth=("elastic", config["cloud_password"])
)

symbol = sys.argv[1]
day = su.get_ts( sys.argv[2] )
end = su.get_ts( sys.argv[3] )
backtest_id = sys.argv[4]

query = {"size": 10000 , "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"minimum_should_match": 1}}, { "term": {"symbol.keyword" : symbol} }, {
    "range": {"open_time": {"gte": f"{day}", "lte": f"{end}", "format": "strict_date_optional_time"}}}]}}}
all = eu.es_search("aug-symbols-1h", query)['hits']['hits']

total = len(all)
su.start_progress(total)
data = {}
for row in all:
    _id = row['_id']
    data[ _id ] = row['_source']

trades = {}

query = {"size": 10000 , "sort": [{"open_time": {"order": "asc"}}], "query": {"term": {"backtest": {"value": backtest_id }}}}
results = eu.es_search(strategy_name, query)['hits']['hits']

for r in results:
    doc = r['_source']
    _id = f"{doc['symbol']}_{su.get_yyyymmdd_hhmm(doc['open_time'])}_1h"
    if _id not in data: continue
    s = doc['symbol']
    ot = doc['open_time']
    if 'ml5' in doc:
        inf = doc['ml5']
        _i = 0
        if inf['top_classes'][0]['class_name'] == 1:
            _i = 0
        else:
            _i = 1

        should_buy = bool(inf['future.1h.buy50_prediction'])
        inf = inf['top_classes'][_i]

        if should_buy and inf['class_probability'] > 0.5:
            aug1d = data[_id]
            if aug1d['bb5'] > 2:
                trades[f"{s}_{su.get_yyyymmdd(ot)}"] = aug1d
    else:
        print(f"{_id} ML field is missing")

    if 'ml' in doc:
        inf = doc['ml']['inference']
        #print(f"{su.get_yyyymmdd(ot)} {s} inf={inf}")
        _i = 0
        if inf['top_classes'][0]['class_name'] == 1:
            _i = 0
        else:
            _i = 1
        #print(f"Call {Fore.GREEN}Elastic Machine Learning{Style.RESET_ALL} - Pipeline Inference Processor")
        should_buy = bool(inf['future.1h.buy10_prediction'])
        inf = inf['top_classes'][_i]

        if should_buy and inf['class_probability'] > 0.5:
            aug1d = data[_id]
            if aug1d['bb5'] > 2:
                trades[f"{s}_{su.get_yyyymmdd(ot)}"] = aug1d

    else:
        print(f"{_id} ML field is missing")

balance = 10000
for t in trades:
    trade = trades[t]
    price = trade['close']
    price1h = trade['future']['1h']['close']['p']
    d = su.delta(price, price1h)
    balance = balance - comission(balance)
    balance = comission(balance, pd=d)
    print(f"{su.get_yyyymmdd(trade['open_time'])} s={trade['symbol']} price={price} 1h={price1h} d={d:1.2f}% b={balance}")

print(f"balance={balance}")