'''
Esse código é focado em apenas 1 symbol/pair por vez e apenas 1 período (1m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1m) por vez
Identificar quando o symbol foi listado na Exchange (Binance/Coinbase)
Verificar qual foi o último registro baixado
Iniciar o processo de download a partir registro imediatamente após o último baixado
Determinar o timestamp atual para servir de ponto de referencia para o termino
Terminar o processo ao baixar o último candle fechado referente ao período em questão

python3 DonwloadSymbol.py <BTCUSDT> <1d|4h|1h|15m|5m|1m>

'''
import sys
import time
import ScientistUtils as su
import datetime
from datetime import timezone
import subprocess
import re
import os
import pytz
from datetime import timedelta

class DownloadSymbol(object):

    def __init__(self):
        self.crypto = sys.argv[1]
        self.pair = sys.argv[2]
        self.cs = sys.argv[3]
        self.symbol = f"{self.crypto}{self.pair}"
        self.filename = f'results/{self.crypto}.out'
        self.start_time = "20211109_0000"

    def download_candles(self, start_time):
        # prepare empty result
        url = f"https://api.binance.com/api/v3/klines?symbol={self.symbol}&interval={self.cs}&startTime={start_time}"
        lines = None
        for i in (1, 2, 3):
            try:
                lines = su.call_binance(url)
                break
            except ConnectionError as ce:
                print(f"ConnectionError {ce.strerror}")
                print("waiting 5 seconds to call binance again")
                time.sleep(5)

        return lines

    def next_time(self):
        line = subprocess.check_output(['tail', '-1', self.filename ])
        latestTimeStr = re.search(rb'\S - \[(\d+),', line)
        if latestTimeStr is not None:
            latestTimeStr = latestTimeStr.group(1).decode('utf8')
            #print(latestTimeStr)
            final_time = su.add_mins_fromtimestamp(float(latestTimeStr),1)
            #print(final_time)
            #print(su.get_iso_datetime(float(final_time)/1000))
            return final_time
        return None

    def get_start_time(self):
        next_time = self.next_time()
        if next_time is None:
            return su.get_ts_yyyymmdd_hhmm(self.start_time) * 1000
        return next_time

    def call(self, timeline):
        klines = self.download_candles(timeline)
        with open(self.filename, 'a') as file:
            for k in klines:
                str_time = su.get_iso_datetime( k[0] / 1000 )
                now_utc_timestamp_str = su.get_iso_datetime( int( datetime.datetime.now().timestamp() ) )
                #print(str_time)
                #print(now_utc_timestamp_str)
                if str_time.strip() != now_utc_timestamp_str.strip():
                    file.write(f"{self.crypto}{self.pair} {self.cs} {su.get_iso_datetime( k[0] / 1000 )} - {k}\n")

    def remove_last_line(self):
        with open(self.filename, "r+", encoding="utf-8") as file:
            file.seek(0, os.SEEK_END)
            pos = file.tell() - 10
            while pos > 0 and file.read(1) != "\n":
                pos -= 1
                file.seek(pos, os.SEEK_SET)
            if pos > 0:
                file.seek(pos, os.SEEK_SET)
                file.truncate()
            file.write(f"\n")

    def start(self):
        #self.remove_last_line()
        next_timestamp_str = self.get_start_time()

        next_time_str = su.get_iso_datetime( int(next_timestamp_str) / 1000)
        #print(next_time_str)

        now_utc_time_str = su.get_iso_datetime(int(datetime.datetime.now().timestamp()))
        #print(now_utc_time_str)

        while( now_utc_time_str.strip() != next_time_str ):
            self.call( next_timestamp_str )

            next_timestamp_str = self.next_time()
            next_time_str = su.get_iso_datetime(int(next_timestamp_str) / 1000)

            now_utc_time_str = su.get_iso_datetime(int(datetime.datetime.now().timestamp()))

            print(f'now_utc_timestamp: {now_utc_time_str.strip()}')
            print(f'next_time:         {next_time_str.strip()}')

    def get_size(self, fileobject):
        fileobject.seek(0,2) # move the cursor to the end of the file
        size = fileobject.tell()
        return size

if __name__ == '__main__':
    DownloadSymbol().start()