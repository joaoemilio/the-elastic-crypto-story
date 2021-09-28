from elasticsearch import helpers, Elasticsearch
import elasticsearch
import ScientistUtils as su 
import logging
import time

############ ELASTICSEARCH ##############
def es_search( iname, query, es="ml-demo" ):
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

def es_exists(iname, id, es="ml-demo"):
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

def es_get(iname, id, es="ml-demo"):
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

def es_create( iname, _id, obj, es="ml-demo" ):
    for i in (1,2,3):
        try:
            elastic[es].create( id=_id, body=obj, index=iname)
            break
        except elasticsearch.exceptions.ConnectionTimeout as cte:
            logging.info( f"Try {i}: {cte.error} elasticsearch.exceptions.ConnectionTimeout")
            logging.info( "waiting 10s before retry sending docs to elasticsearch")
            time.sleep(10)

def es_bulk_create_multi_index(index_data, partial=1000, es="ml-demo"):
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

def es_bulk_create(iname, data, partial=100, es="ml-demo", pipeline=None):
    count = 1
    if not partial: partial = len(data)
    actions = []
    for k in data:
        action = { "_index": iname, "_source": data[k], "id": k, "_id": k }
        actions.append( action )
        if partial and count == partial:

            for i in (1,2,3):
                try:
                    print(f"uploading {partial} documents")
                    helpers.bulk(client=elastic[es],actions=actions, pipeline=pipeline)
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


def es_bulk_update_multi_index(index_data, partial=500, es="ml-demo"):
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

def es_bulk_update(iname, data, partial=100, es="ml-demo"):
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


config = su.read_json( f"config/config.json" )

es = Elasticsearch(    
    cloud_id= config["cloud_id"] ,
    http_auth=("elastic", config["cloud_password"])
)

es_u = Elasticsearch(
    cloud_id= config["cloud_id_upload"] ,
    http_auth=("elastic", config["cloud_password_upload"])
)
elastic = { "ml-demo": es, "ccr-demo": es_u}

