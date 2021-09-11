
from collections import deque
import numpy as np


def transpose(data):
    data2 = {}
    for cs in data:
        dcs = data[cs]
        data2[cs] = transpose_dcs(dcs)
    return data2

def transpose_dcs(dcs):
    dcs2 = {}
    for ta in dcs:
        for s, o in ta[1].items():
            if s not in dcs2:
                dcs2[s] = []
            dcs2[s].append(o)
    return dcs2

def calculate_d0(candles):
    c0 = None
    for c in candles:
        v0 = c['open']
        v1 = c['close']
        if c0: v0 = c0['close']
        c['d0'] = (v1 - v0)/v0
        c0 = c

def calculate_dlow(candles):
    for c in candles:
        c['dlow'] = (c['close'] - c['low'])/c['close']

def calculate_mms(candles, size):
    metrics = ["d0", "close", "q_volume", "trades"]
    # metrics = ["d0", "close", "q_volume", "trades", "dlow"] FIXME the rollout needs to delete 1d SlimProphet data
    q = deque()
    c1 = None
    for c in candles:
        if len(q) == size:
            q.popleft()
        q.append(c)
        for m in metrics:
            if len(q) == size:
                c[f"{m}_mm{size}"] = np.mean([c[m] for c in q])
                if m == "close":
                    if c1: 
                        v1 = c1[f"close_mm{size}"]
                        v2 = c[f"close_mm{size}"]
                        c[f"d_close_mm{size}"] = (v2 - v1)/v1
                    else:
                        c[f"d_close_mm{size}"] = None
            else:
                c[f"{m}_mm{size}"] = None
                c[f"d_close_mm{size}"] = None

        if c[f"close_mm{size}"]: c1 = c

def calculate_bb(candles, size=20):
    q = deque()
    for c in candles:
        q.append(c)
        if len(q) == size:
            closes = [c['close'] for c in q]
            c[f"mid_bb{size}"] = np.mean(closes)
            c[f"std{size}"] = np.std(closes)
            q.popleft()
        else:
            c[f"mid_bb{size}"] = None
            c[f"std{size}"] = None

def calculate_pump(candles, size=25):
    q = deque()
    for c in candles:
        q.append(c)
        if len(q) == size:
            vols = [c['q_volume'] for c in q]
            mi = min(vols)
            c[f"pump{size}"] = None if mi == 0 else max(vols)/mi
            q.popleft()
        else:
            c[f"pump{size}"] = None

def rsi0(q):
    size = len(q)
    changes = [ o["close"] - o["close"]/(o["d0"]+1) for o in q ]
    gain = sum( c for c in changes if c > 0 )/size
    loss = sum( -c for c in changes if c < 0 )/size

    rsi = 100
    if loss > 0:
        rs = gain/loss
        rsi = 100 - 100/(1+rs)

    return rsi

def calculate_rsi(candles, size=14):
    q = deque()
    for c in candles:
        q.append(c)
        if len(q) == size:
            c[f'rsi{size}'] = rsi0(q)
            q.popleft()
        else:
            c[f'rsi{size}'] = None


