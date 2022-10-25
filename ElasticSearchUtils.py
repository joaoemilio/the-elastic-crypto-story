from elasticsearch import helpers, Elasticsearch
import elasticsearch
import TECSUtils as su 
import logging
import time

############ ELASTICSEARCH ##############
def es_search( iname, query, es="tecs" ):
    results = None
    for i in (1,2,3):
        try:
            results = elastic[es].search( index=iname, body=query)
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)
    return results

def es_exists(iname, id, es="tecs"):
    results = None
    for i in (1,2,3):
        try:
            results = elastic[es].exists( index=iname, id=id)
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)

    return results

def es_get(iname, id, es="tecs"):
    results = None
    for i in (1,2,3):
        try:
            results = elastic[es].get( id=id, index=iname)
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)
    return results

def es_create( iname, _id, obj, es="tecs", pipeline=None ):
    res = None
    for i in (1,2,3):
        try:
            if pipeline:
                res = elastic[es].create( id=_id, body=obj, index=iname, pipeline=pipeline)
            else:
                res = elastic[es].create( id=_id, body=obj, index=iname)

            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)
    return res

def es_bulk_create_multi_index(index_data, partial=1000, es="tecs"):
    actions = []
    count = 0
    for iname in index_data:
        data = index_data[iname]
        for k in data:
            action = { "_index": iname, "_source": data[k], "id": k, "_id": k }
            actions.append( action )
            count += 1
            if count >= partial:
                for i in (1,2,3):
                    try:
                        helpers.bulk(elastic[es],actions=actions)
                        actions = []
                        count = 0
                        break
                    except elasticsearch.exceptions.ConnectionTimeout as cte:
                        logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
                        logging.info( "waiting 10s before retry sending docs to elasticsearch")
                        time.sleep(10)

    for i in (1,2,3):
        try:
            helpers.bulk(client=elastic[es],actions=actions)
            actions = []
            count = 0
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)

def es_bulk_create(iname, data, partial=100, es="tecs", pipeline=None):
    count = 1
    if not partial: partial = len(data)
    actions = []
    for k in data:
        action = { "_index": iname, "_source": data[k], "id": k, "_id": k, "pipeline": pipeline }
        actions.append( action )
        if partial and count == partial:

            for i in (1,2,3):
                try:
                    print(f"uploading {partial} documents")
                    helpers.bulk(client=elastic[es],actions=actions)
                    break
                except elasticsearch.exceptions.ConnectionTimeout as cte:
                    logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
                    logging.info( "waiting 10s before retry sending docs to elasticsearch")
                    time.sleep(10)
            actions = []
            count = 0
        else:
            count += 1

    for i in (1,2,3):
        try:
            print(f"uploading {count} documents")
            helpers.bulk(client=elastic[es],actions=actions)
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)


def es_bulk_update_multi_index(index_data, partial=500, es="tecs"):
    actions = []
    count = 0
    for iname in index_data:
        data = index_data[iname]
        for k in data:
            action = { "_op_type": "update", "_index": iname, "_id": k, "doc": data[k] }
            actions.append( action )
            count += 1
            if count >= partial:
                for i in (1,2,3):
                    try:
                        logging.info( f"Uploading {count} docs to Elastic Cloud")
                        helpers.bulk(client=elastic[es],actions=actions)
                        actions = []
                        count = 0
                        break
                    except elasticsearch.exceptions.ConnectionTimeout as cte:
                        logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
                        logging.info( "waiting 10s before retry sending docs to elasticsearch")
                        time.sleep(10)

    for i in (1,2,3):
        try:
            logging.info( f"Uploading {len(actions)} docs to Elastic Cloud")
            helpers.bulk(client=elastic[es],actions=actions)
            actions = []
            count = 0
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)

def es_bulk_update(iname, data, partial=100, es="tecs"):
    count = 1
    if not partial: partial = len(data)
    actions = []
    for k in data:
        action = { "_op_type": "update", "_index": iname, "_id": k, "doc": data[k] } 
        actions.append( action )
        if partial and count == partial:

            for i in (1,2,3):
                try:
                    helpers.bulk(client=elastic[es],actions=actions)
                    break
                except elasticsearch.exceptions.ConnectionTimeout as cte:
                    logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
                    logging.info( "waiting 10s before retry sending docs to elasticsearch")
                    time.sleep(10)
                except elasticsearch.exceptions.RequestError as ree:
                    logging.error(f"Could not bulk update documents for {k} and other docs together")
                    logging.error(ree)
            actions = []
            count = 0
        else:
            count += 1

    for i in (1,2,3):
        try:
            helpers.bulk(client=elastic[es],actions=actions)        
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)
        except elasticsearch.exceptions.RequestError as ree:
            logging.error(f"Could not bulk update documents for {k} and other docs together")
            logging.error(ree)

def es_index( iname, _id, obj, es="tecs", pipeline=None ):
    res = None
    for i in (1,2,3):
        try:
            if pipeline:
                res = elastic[es].index( id=_id, body=obj, index=iname, pipeline=pipeline)
            else:
                res = elastic[es].index( id=_id, body=obj, index=iname)

            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)
    return res

def get_last_n_docs(symbol, cs, ts_start, window_size):
    periods = {"5m": 60*5,  "15m": 60*15,
               "1h": 60*60, "4h": 4*60*60, "1d": 24*60*60}
    ts_window_size = ts_start-window_size*periods[cs]

    query = {"size": window_size, "sort": [{"open_time": {"order": "asc"}}], "query": {"bool": {"filter": [{"bool": {"should": [{"match_phrase": {"symbol.keyword": symbol}}], "minimum_should_match": 1}}, {
        "range": {"open_time": {"gte": f"{ts_window_size}", "lte": f"{ts_start}", "format": "strict_date_optional_time"}}}]}}}
    results = es_search(f"symbols-{cs}", query)
    data = {}
    if 'hits' in results and 'hits' in results['hits']:
        r = results['hits']['hits']        
        for d in r:
            data[d['_id']] = d['_source']

    return data

config = su.read_json( f"../config.json" )

es = Elasticsearch(    
    cloud_id= config["elastic_cloud_id"] ,
    http_auth=("elastic", config["elastic_cloud_password"])
)

elastic = { "tecs": es }

