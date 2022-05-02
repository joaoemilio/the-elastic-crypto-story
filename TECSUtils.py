import numpy as np
import json 

import logging
import requests
import os
from os import listdir
from os.path import isfile, join, exists
from datetime import datetime, timezone, timedelta
import time
import pickle
import dateutil.parser as dtparser
from binance.client import Client 
import boto3
import botocore

def file_exists(fname):
    return exists(fname)

def read_txt(fname):

    lines = []
    with open(fname, "r") as f:
        line = f.readline()
        while line:
            lines.append( line )
            line = f.readline()

    return "".join(lines)

def create_dir(path):
    isExist = os.path.exists(path)
    if not isExist:
        # Create a new directory because it does not exist 
        os.makedirs(path)

def write_json(obj, fname):
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)
        
def read_json(fname):
    with open(fname, 'r') as json_file:
        return json.load(json_file)

def load_symbols():
    # Seleted symbols with q_volume_mm7 > 10e6 on 20210615, excluding BLVT and stable coins
    return read_json("symbols.json")

def get_config():
    return read_json( f"../config.json" )

def log(msg, fn=''):
    logging.info(f'{msg}')

def debug(msg):
    logging.debug(f'{msg}')


def call_binance(url):
    payload={}
    headers = {
    'Content-Type': 'application/json'
    }
    
    '''
    A 429 will be returned when either rate limit is violated
    A 418 is an IP ban from 2 minutes to 3 days
    '''
    backoff = 30
    status_code = -1
    response = None
    while status_code == -1 or status_code == 429:
        response = requests.request("GET", url, headers=headers, data=payload) # response: status_code:int, json:method, text:str
        status_code = response.status_code
        if status_code == 429:
            if backoff > 100:
                raise BaseException("ERROR: Too many HTTP 429 responses")
            logging.warn(f'WARN: Sleeping {backoff}s due to HTTP 429 response')
            time.sleep(backoff)
            backoff = 2*backoff

    if status_code != 200:
        err = f"ERROR: HTTP {status_code} response for {url}"
        if status_code == 418:
            err = "ERROR: Binance API has banned this current IP"    
        logging.error(err)
        raise BaseException(err)
    return response.json()   

def get_yyyymmdd(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y%m%d")

def get_yyyymmdd_hhmm(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y%m%d_%H%M")

def get_yyyymmdd_hh00(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y%m%d_%H00")

def get_iso_datetime(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M")
    
def get_ymdHM(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y%m%d_%H%M")
    
def get_ts(yyyymmdd):
    yyyymmdd = f'{yyyymmdd}'
    t = f'{yyyymmdd[0:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}T00:00:00+00:00'
    return int(datetime.timestamp(dtparser.isoparse(t)))

def get_ts_yyyymmdd_hh00(yyyymmdd_hhmm):
    t = f'{yyyymmdd_hhmm[0:4]}-{yyyymmdd_hhmm[4:6]}-{yyyymmdd_hhmm[6:8]}T{yyyymmdd_hhmm[9:11]}:00:00+00:00'
    return int(datetime.timestamp(dtparser.isoparse(t)))

def get_ts_yyyymmdd_hhmm(yyyymmdd_hhmm):
    t = f'{yyyymmdd_hhmm[0:4]}-{yyyymmdd_hhmm[4:6]}-{yyyymmdd_hhmm[6:8]}T{yyyymmdd_hhmm[9:11]}:{yyyymmdd_hhmm[11:13]}:00+00:00'
    return int(datetime.timestamp(dtparser.isoparse(t)))

def get_ts2(tiso:str):
    # tiso must be like 2021-06-25T23:53
    tiso = tiso + ":00+00:00"
    return int(datetime.timestamp(dtparser.isoparse(tiso)))

def get_iso_datetime_sec(ts):
    dt = datetime.fromtimestamp(ts,tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


#############################
# Technical analisys indicators
#############################
def moving_avg(number, numbers, window_size):
    if window_size == 0:
        return 0
    numbers.append(number)
    n = numbers[-window_size:]
    mma = sum(n) / window_size
    return mma

def delta(starting_price, closing_price):
    return 0 if starting_price == 0 else (closing_price - starting_price)/starting_price

def std_dev(number, numbers, window_size):
    numbers.append(number)
    n = numbers[-window_size:]
    return np.std(n)

def mean(number, numbers, window_size):
    numbers.append(number)
    n = numbers[-window_size:]
    return np.mean(n)

def dp(current_price, previous_price, ):
    return 0 if previous_price == 0 else 100*(current_price - previous_price)/previous_price

def bb(current_price, mov_avg_price, std_price):
    return 0 if std_price == 0 else (current_price - mov_avg_price) / std_price

config = read_json("../config.json")
session = boto3.Session(
    aws_access_key_id=config["aws_access_key_id"],
    aws_secret_access_key=config["aws_secret_access_key"]
)
s3 = session.resource('s3', region_name='us-east-1')

def aws_s3_upload(bucket_name, src_file, dest_file):
    log(f"aws_s3_upload {src_file}")
    s3.meta.client.upload_file(src_file, bucket_name, dest_file)

def get_bucket(s3, s3_uri: str):
    """Get the bucket from the resource.
    A thin wrapper, use with caution.

    Example usage:

    >> bucket = get_bucket(get_resource(), s3_uri_prod)"""
    return s3.Bucket(s3_uri)


def isfile_s3(bucket, key: str) -> bool:
    """Returns T/F whether the file exists."""
    objs = list(bucket.objects.filter(Prefix=key))
    return len(objs) == 1 and objs[0].key == key


def isdir_s3(bucket, key: str) -> bool:
    """Returns T/F whether the directory exists."""
    objs = list(bucket.objects.filter(Prefix=key))
    return len(objs) > 1