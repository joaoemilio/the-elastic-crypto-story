
from colorama import Back, init
from colorama import Fore, Style
import sys
import time
import os.path
import os
from pathlib import Path
import fire
from subprocess import Popen
import calendar
import TECSUtils as utils
import CryptoDownload as cd
import BinanceAPI as bapi
import ElasticSearchUtils as eu

# Pro Elalo
from zipfile import ZipFile
import pathlib

usage = utils.read_txt( "TECS.txt" )
#candle_sizes = {'1m':60, '3m': 3*60, '5m': 5*60, '15m': 15*60, '30m': 30*60, '1h': 3600, '2h': 2*3600,  '4h': 4*3600, '6h': 6*3600, '8h': 8*3600, '12h': 12*3600, '1d': 24*3600}
candle_sizes = {'1h': 3600, '2h': 2*3600,  '4h': 4*3600, '6h': 6*3600, '8h': 8*3600, '12h': 12*3600, '1d': 24*3600}

# Initiate colorama to provide color to our terminal outputs
init()


class TECS( object ):
    def __init__(self):
        pass

    def confirm(self, msg):
        """
        Ask user to enter Y or N (case-insensitive).
        :return: True if the answer is Y.
        :rtype: bool
        """
        answer = ""
        while answer not in ["y", "n"]:
            answer = input(f"{msg} [Y/N]? ").lower()
        return answer == "y"

class Crypto(TECS):

    def download_yesterday(self):
        symbols = utils.read_json("symbols.json")
        ymd_yesterday = utils.get_yesterday_yyyymmdd()
        print(f"yesterday is: {ymd_yesterday}")

        for s in symbols:
            for cs in candle_sizes:
                self.download(s, cs, ymd_yesterday )

    

    def download_all(self):
        symbols = utils.read_json("symbols.json")
        for s in symbols:
            for cs in candle_sizes:
                self.download(s, cs)

    def download_all_from(self, ymd_start):
        symbols = utils.read_json("symbols.json")
        for s in symbols:
            for cs in candle_sizes:
                self.download(s, cs, ymd_start)

    def update_list(self):
        symbols1 = bapi.get_symbols()
        if utils.file_exists("symbols.json"):
            symbols2 = utils.read_json("symbols.json")
        else:
            symbols2 = {}

        symbols = {}
        for s in symbols1:
            if s not in symbols2: 
                symbols[s] = "20191231"

        for s in symbols:
            print(f"updating {s}")
            klines = bapi.get_first_kline(s,"1d")
            start = utils.get_yyyymmdd( klines[0][0] / 1000 )
            symbols2[s] = start 
        
        utils.write_json(symbols2, "symbols.json")

    def download( self, symbol, cs, ymd_start:str ):
        symbols = utils.read_json("symbols.json")
        # determine if this symbol is already in the list of available symbols
        if symbol not in symbols: 
            print(f"{symbol} is not in our list. Please execute {Fore.YELLOW}tecs crypto update_list{Style.RESET_ALL} and try again.")
            return 

        # determine when was the symbol launched in Binance
        ts_start = utils.get_ts( ymd_start )
        today = utils.get_ts(utils.get_yyyymmdd(time.time()))

        # end date is yesterday midnight
        t = ts_start
        all_data = {}
        while t < today:
            ymdHM = utils.get_ymdHM(t)
            print(f"Downloading from binance {symbol} {cs} {ymdHM}",end= "\r")
            data = cd.download( symbol=symbol, cs=cs, ts_start=t)
            for k in data:
                o = data[k]
                fpath = f'{config["download_path"]}/{symbol}/{cs}'
                fname = f'{fpath}/{ymdHM}.json'
                utils.create_dir(fpath)
                print(f"Creating json file {symbol} {cs} {ymdHM}",end= "\r")
                utils.write_json( o, fname )
            
                print(f"Uploading to Elasticsearch {symbol} {cs} {ymdHM}",end= "\r")
                eu.es_index(f"crypto-{symbol.lower()}-{cs}", k, o)

            t += (candle_sizes[cs])
        print(f"\n")
        print_fg(Fore.YELLOW, f"Download klines from binance is complete")

    def download_from_day_one( self, symbol, cs ):
        symbols = utils.read_json("symbols.json")
        # determine if this symbol is already in the list of available symbols
        if symbol not in symbols: 
            print(f"{symbol} is not in our list. Please execute {Fore.YELLOW}tecs crypto update_list{Style.RESET_ALL} and try again.")
            return 

        self.download(symbol, cs, symbols[symbol] )

    def usd( self, symbol, usd ):
        answer = self.confirm( Fore.WHITE + f"Confirm: buying ${usd:1.3f} of {symbol} for {self.customer_key}?" )
        if answer:
            buy = self.bo.execute_mkt_buy( symbol, usd )
            print( f"Buy operation has been {Fore.YELLOW} processed{Style.RESET_ALL}. Check details bellow:\n" )
            print( Fore.WHITE + f"{symbol} - Qty: {Fore.GREEN} {buy['qty']} {Style.RESET_ALL} at Avg Price: {Fore.GREEN} {buy['pbuy1']}" )
        else:
            print( Fore.RED, "Buy operation has been cancelled" )


def print_bg( _color, text ):
    print( _color + text )
    print(Style.RESET_ALL)

def print_fg( _color, text ):
    print( _color + text )
    print( Style.RESET_ALL)

config = utils.read_json("../config.json")

if __name__ == '__main__':
    apps = {"crypto" : Crypto }
    if len(sys.argv) < 2: 
        print(usage)
        sys.exit() 
    else: 
        app = sys.argv[1]
        sys.argv = sys.argv[1:]

    print_fg(Fore.YELLOW, "----------------------------------------")
    if app in apps:
        fire.Fire( apps[ app ](), name=app )
    else:
        print_bg(Back.RED, f"This app '{app}' does not exist")
    print_fg(Fore.YELLOW, "----------------------------------------")
