from elasticsearch import Elasticsearch
from elasticsearch import exceptions
import json 
import time
from colorama import Fore, Back, Style
import sys
import ScientistUtils as su 
import ElasticSearchUtils as eu

def read_json(fname):
    with open(fname, 'r') as json_file:
        return json.load(json_file)

config = read_json( f"config/config.json" )
es = Elasticsearch(    
    cloud_id= config["cloud_id"] ,
    http_auth=("elastic", config["cloud_password"])
)

#pipeline = "buy-bnb-1h-half-percent"
pipeline = "pipeline-bnb-1hbuy10"

# PYTHON ELASTICSEARCH CLIENT - CONNECTS TO ELASTIC CLOUD
def buy_bnb( index, doc ):
    resp = es.create( body=doc, id=time.time(), index=index, pipeline=pipeline, request_timeout=30)
    _doc = es.get(id=resp["_id"], index=index)
    doc = _doc['_source']
    inf = doc['ml5']['inference']
    _i = 0
    if inf['top_classes'][0]['class_name'] == 1:
        _i = 0
    else:
        _i = 1
    #print(f"Call {Fore.GREEN}Elastic Machine Learning{Style.RESET_ALL} - Pipeline Inference Processor")
    #future.1h.buy10_prediction
    buybnb = bool(inf['future.1h.buy50_prediction'])
    print(f"{Fore.YELLOW} {su.get_iso_datetime(doc['open_time'])} -> Buy ETH?: {Style.RESET_ALL}{'True' if buybnb else 'False'}\n")

    return buybnb, inf['top_classes'][_i]

def comission(b, pd=None):
    if not pd:
        return b * 0.075/100
    else:
        _b = b * pd
        b = b+_b
        b = b - comission(b)
        return b

def get_1h_docs(symbol, ts_start, ts_end, backtest):
    data = {}
    fields = su.read_json(f"config/ethusdt/fields.json")['includes']
    while ts_start < ts_end:
        query = {"size": 1000, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {
            "range": {"open_time": {"gte": f"{ts_start}", "lte": f"{ts_end}", "format": "strict_date_optional_time"}}}]}}}
        results = eu.es_search("aug-symbols-1h", query)['hits']['hits']
        for d in results:
            doc = {}
            for f in fields:
                if f not in d['_source']: 
                    print(f"{f} not found {d['_source']}")
                    continue
                doc[f] = d['_source'][f]
            doc['backtest'] = backtest
            data[d['_id']] = doc 
            last_id = d['_id']

        print(f"loaded {len(data)} docs from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(data[last_id]['open_time'])}")
        ts_start = data[last_id]['open_time']

    return data
symbol = sys.argv[1]
start_day = sys.argv[2]
day = su.get_ts(start_day)
end_day = sys.argv[3]
end = su.get_ts(end_day)
backtest = int(time.time())
data = get_1h_docs( symbol, day, end, backtest )
eu.es_bulk_create(iname="eth-five-percent-in-one-hour", data=data, partial=100, pipeline=pipeline)
# for k in data:
#     _id = f"{k}_{backtest}"
#     try:
#         res = eu.es_create(iname="eth-five-percent-in-one-hour", _id=_id, obj=data[k], pipeline=pipeline )
#         print(res['_id'])
#     except Exception as e:
#         print(e)

print(f"python3 BacktestReportSymbol.py {symbol} {start_day} {end_day} {backtest}")