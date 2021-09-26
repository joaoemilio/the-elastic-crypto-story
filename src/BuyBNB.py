from elasticsearch import Elasticsearch
import json 
import time
from colorama import Fore, Back, Style
import ScientistUtils as su 

def read_json(fname):
    with open(fname, 'r') as json_file:
        return json.load(json_file)

config = read_json( f"config.json" )
es = Elasticsearch(    
    cloud_id= config["cloud_id"] ,
    http_auth=("elastic", config["cloud_password"])
)

pipeline = "buy-bnb-1h-half-percent"

# PYTHON ELASTICSEARCH CLIENT - CONNECTS TO ELASTIC CLOUD
def buy_bnb( index, doc ):
    resp = es.create( body=doc, id=time.time(), index=index, pipeline=pipeline, request_timeout=30)
    _doc = es.get(id=resp["_id"], index=index)
    doc = _doc['_source']
    inf = doc['ml']['inference']
    _i = 0
    if inf['top_classes'][0]['class_name'] == 1:
        _i = 0
    else:
        _i = 1
    #print(f"Call {Fore.GREEN}Elastic Machine Learning{Style.RESET_ALL} - Pipeline Inference Processor")
    buybnb = bool(inf['future.1h.buy_prediction'])
    print(f"{Fore.YELLOW} {su.get_iso_datetime(doc['open_time'])} -> Buy BNB?: {Style.RESET_ALL}{'True' if buybnb else 'False'}\n")

    return buybnb, inf['top_classes'][_i]

def comission(b, pd=None):
    if not pd:
        return b * 0.075/100
    else:
        _b = b * pd
        b = b+_b
        b = b - comission(b)
        return b


def get_1m_docs(symbol, ts_start, ts_end, backtest):
    data = {}
    fields = su.read_json(f"../config/bnbusdt/d_vol_model.json")['includes']
    while ts_start < ts_end:
        query = {"size": 1000, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {
            "range": {"open_time": {"gte": f"{ts_start}", "lte": f"{ts_end}", "format": "strict_date_optional_time"}}}]}}}
        results = su.es_search("symbols-1m-aug", query)['hits']['hits']
        for d in results:
            doc = {}
            for f in fields:
                if f not in d['_source']: continue
                cs = f.split(".")[0]
                f1 = f.split(".")[1]
                doc[f] = d['_source'][cs][f1]
            doc['backtest'] = backtest
            doc['open_time'] = d['_source']['open_time']
            data[d['_id']] = doc 
            last_id = d['_id']

        print(f"loaded {len(data)} docs from {su.get_iso_datetime(ts_start)} to {su.get_iso_datetime(data[last_id]['open_time'])}")
        ts_start = data[last_id]['open_time']

    return data

day = su.get_ts("20210920")
end = su.get_ts("20210925")
backtest = time.time()
while day < end:
    data = get_1m_docs("BNBUSDT", day, end, backtest )
    su.es_bulk_create("backtest-bnbusdt", data, partial=10, pipeline=pipeline)

'''
buys = {}
b1 = 100
b2 = 100
b1h = 100
b2h = 100

while day < end:
    _id = f"BNBUSDT_{su.get_yyyymmdd_hhmm(day)}_1m"
    doc = su.es_get("symbols-1m-aug", _id)["_source"]
    buy, prediction = buy_bnb("buybnb", doc )
    if buy:
        b1 = b1 - comission(b1)
        b2 = b2 - comission(b2)
        b1h = b1h - comission(b1h)
        b2h = b2h - comission(b2h)
        buys[_id] = { "pbuy": doc['close'], 'tbuy': doc['open_time'], 'ptarget': doc['future']['1h']['close']['p'], 'pd': doc['future']['1h']['close']['d'], "prob": prediction['class_probability'], "score": prediction['class_score'], 'high': doc['future']['1h']['high']['p'], "hd": doc['future']['1h']['high']['d'] }
        b1 = comission(b1, pd=buys[_id]['pd'])
        b2 = comission(b2, pd=buys[_id]['pd'])
        b1h = comission(b1h, pd=buys[_id]['hd'])
        b2h = comission(b2h, pd=buys[_id]['hd'])
        buys[_id]['b1'] = b1
        buys[_id]['b2'] = b2
        buys[_id]['b1h'] = b1h
        buys[_id]['b2h'] = b2h

    day = day+ 60*60 

for b in buys:
    print(buys[b])
'''