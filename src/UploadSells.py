import ScientistUtils as su
import json 
import glob

def get_strategy(fname:str):
    stc = fname.replace("sell-", "").replace(".txt", "")
    customer = stc.split("-")[0]
    stc = stc.replace(customer+"-", "")
    return stc

def load_trades(c:str):
    # get all sell logs for the customer
    fsells = glob.glob(f'../sells/sell-{c}-*.txt')

    # load all trades for the customer
    trades = []
    for fname in fsells:
        lines = []
        with open(fname, "r") as f: 
            for line in f.readlines():
                line = line.replace("}{", "}\n{")
                lines += line.split("\n")

        _trades = [json.loads(line) for line in lines if len(line) > 10]
        for t in _trades:
            if "strategy" not in t:
                t["strategy"] = get_strategy(fname)
        trades += _trades
    
    # fix the tsell1 format
    for tr in trades:
        try:
            tr["tsell1"] = su.get_iso_datetime_sec(tr['tsell1']/1000)
        except:
            pass

    # sort trades by tsell
    trades = sorted(trades, key=lambda trade: trade['tsell1']) 

    return trades

trades = load_trades("joaoemilio")
for t in trades:
    tbuy = t["tbuy"]
    h = tbuy[11:13]
    min = tbuy[14:16]
    t["hour"] = h
    t["min"] = min
    t["trader"] = "joaoemilio"
    _id = f"{t['trader']}_{t['symbol']}_{t['tbuy']}"
    su.es_create("trades", _id, t )
