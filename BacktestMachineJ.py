'''
The backtest machine performs backtests based on Binance cripto currency historical data
'''
import os
import os.path
import ProphetUtils as pu 
import ReportBacktest as rb

##################################################
# Data load
##################################################

def iso(t):
    return pu.get_iso_datetime(t)

def get_fname_aug(day, cs):
    day = pu.get_yyyymmdd(day)
    return f"j/data/aug/{day}-{cs}.json"

def load_data(start, end=None):
    data = {}

    # load data for the time_interval
    # if not enough data in files, raise error 
    day = start
    files_count = 0
    while day <= end:
        for k in pu.candle_sizes1m:
            fname = get_fname_aug(day, k)
            if not os.path.exists(fname): 
                raise Exception(f'File does not exist: {fname}. Download first')
            if k not in data:
                data[k] = []
            dcs = pu.read_json(fname)
            data[k] += dcs
            files_count += 1
        day += 3600*24
        
    pu.log(f"Data is loaded, files_count={files_count}", "load_data")

    return data


##################################################
# Main algorithm
##################################################

def execute_backtest(strategy, data ):
    '''
    :returns: {cs:[ [open_ts, {d0, balance}], ..]}
    '''

    pu.log(f'Starting backtest', strategy.desc())

    # initialize data setting buy=False
    strategy.init_backtest(data)
    pu.log(f'Data initialized', strategy.desc()) 

    # execute backtests
    for i, t in enumerate(data['1m']):
        if i < 24*60:
            continue # the first day must be ignored

        tint = t[0]

        # actually executes for each interval
        if tint % int(24*3600) == 0:
            strategy.process1d(tint)

        if tint % int(4*3600) == 0:
            strategy.process4h(tint)
        
        if tint % int(3600) == 0:
            strategy.process1h(tint)
        
        if tint % int(15*60) == 0:
            strategy.process15m(tint)
        
        if tint % int(5*60) == 0:
            strategy.process5m(tint)
        
        strategy.process1m(tint)


    strategy.close_backtest()
    pu.log(f'Backested finished', strategy.desc())


data = None
def execute_backtests(start, end=None, strategies=[], refresh_data=False, month=None):
    '''
    data format:
    {
        '1d': [[open_time, {symbol:{open, high, low, close, q_volume, trades, open_time, 
            d0_mm7, close_mm7, q_volume_mm7, trades_mm7, d1_mm7, close1_mm7, q_volume1_mm7, trades1_mm7}, ..}], ..],
        '4h': [..],
        '1h': [..],
        '15m': [..],
        '5m': [..]
    } 
    '''

    end = end if end else start

    print('--------------------------------------------------------------------------------')
    print(f"START Backtests from {pu.get_iso_datetime(start)} to {pu.get_iso_datetime(end)}")
    print('--------------------------------------------------------------------------------')

    pu.log(f"Lets load data", "execute_backtests")
    global data
    if not data or refresh_data:
        data = load_data(start=start,end=end)

    # Generate reports
    pu.log(f"Generate reports", "execute_backtests")
    rb.cleanup_reports()
    for st in strategies:
        execute_backtest(st, data)
        rb.report_backtest(st.desc())    
    rb.report_comparison(strategies, start, end)
    rb.backup_reports()


def execute_backtests_b(yyyymmdd_start, yyyymmdd_end=None, strategies=[], refresh_data=False, month=None):
    if not yyyymmdd_end:
        yyyymmdd_end = yyyymmdd_start
    ts_start = pu.get_ts(yyyymmdd_start)
    ts_end = pu.get_ts(yyyymmdd_end)

    execute_backtests(ts_start, ts_end, strategies, refresh_data, month)

