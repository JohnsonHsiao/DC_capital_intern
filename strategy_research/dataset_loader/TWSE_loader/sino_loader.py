import os, sys
import shioaji as sj

sys.path.insert(1, os.path.dirname(__file__) + '/../..')
os.environ['TZ'] = 'Asia/Hong_Kong'

import platform 
import time
if platform.system() == "Darwin":
    time.tzset() # time zone setting

from datetime import datetime
import pandas as pd
import numpy as np
import configparser
from enum import Enum
from datetime import datetime

from ..csv_dealer import CsvDealer

class SinoLoader:
    def __init__(self):
        path_config = configparser.ConfigParser()
        path_config.read(f'{os.path.dirname(__file__)}/../config/path_config.ini')
        # self.FUTURE_SINCE = datetime(2020, 3, 23).strftime('%Y-%m-%d')
        # self.EQUITY_SINCE = datetime(2018, 12, 7).strftime('%Y-%m-%d')
        self.EQUITY_SINCE = '2023-01-01'
        self.TODAY = datetime.now().strftime('%Y-%m-%d')
        self.api = sj.Shioaji()
        self.api.login(api_key="ASfM6JmMsn4vqE5s3dtRvbNFfLJo2qFzgQPA83LrofZp",
                       secret_key="GorietWLueyyniirVhcJZUbVcy51LUaqR7UAFfDucY2N",
                       contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done."))    

    def sino_logout(self):
        self.api.logout()
        print('SINO Logout!')

    def download_future_df(self):
        self.sino_login(self.person_id, self.password)
        os.makedirs(self.path_future, exist_ok=True)
        for symbol in FUTURE_TICKER:
            print(f'Loading {symbol}...')
            file = f'{self.path_future}/{symbol}_future_1m.csv'
            if os.path.isfile(file):
                FORMAT = '%Y-%m-%d %H:%M:%S'
                csv_writer = CsvDealer(file, None, None, FORMAT)
                start_dt = csv_writer.get_last_row_date_csv(format=FORMAT)
                start_day_dt = start_dt.strftime('%Y-%m-%d')
                kbars = self.api.kbars(self.api.Contracts.Futures[symbol], start=start_day_dt, end=self.TO)
                df = pd.DataFrame({**kbars})
                df.ts = pd.to_datetime(df.ts)
                df = df.rename({'ts':'datetime', 'Open':'open', 'Close':'close', 'High':'high', 'Low':'low', 'Volume':'volume'}, axis=1)
                df = df[['datetime','open','high','low','close','volume']]
                for row in df.iterrows():
                    row=row[1]
                    csv_writer.write_if_needed([row[0], row[1], row[2], row[3], row[4], row[5]], start_dt)
            else:
                kbars = self.api.kbars(self.api.Contracts.Futures[symbol], start=self.FUTURE_SINCE, end=self.TO, timeout=300000)
                df = pd.DataFrame({**kbars})
                df.ts = pd.to_datetime(df.ts)
                df = df.rename({'ts':'datetime','Open':'open', 'Close':'close', 'High':'high', 'Low':'low', 'Volume':'volume'}, axis=1)
                df = df[['datetime','open', 'high', 'low', 'close', 'volume']]
                df.to_csv(file, index=False)
        self.sino_logout

    def get_stock_list(self):
        stock_list = np.sort(
            [x['code'] for x in list(self.api.Contracts.Stocks['TSE']) + list(self.api.Contracts.Stocks['OTC']) if
            (sum(c.isdigit() for c in x['code']) == 4) & (len(x['code']) == 4) &
            (x['category'] != '00') & (x['category'] != '')])
        return stock_list

    def download_tweq_1m(self, stock_list: list = None):
        self.sino_login(self.person_id, self.password)
        os.makedirs('./output/', exist_ok=True)
        os.makedirs('./output/TWSE', exist_ok=True)
        if stock_list == None:
            stock_list = self.get_stock_list()
        for stock in stock_list:
            file = f'./output/TWSE/{stock}_kbar.csv'
            if os.path.isfile(file):
                FORMAT = '%Y-%m-%d %H:%M:%S'
                csv_writer = CsvDealer(file, None, None, FORMAT)
                start_dt = csv_writer.get_last_row_date_csv(format=FORMAT)
                start_day_dt = start_dt.strftime('%Y-%m-%d')
                kbars = self.api.kbars(self.api.Contracts.Stocks[stock], start=start_day_dt, end=self.TO)
                df = pd.DataFrame({**kbars})
                df.ts = pd.to_datetime(df.ts)
                df = df.rename({'ts':'datetime', 'Open':'open', 'Close':'close', 'High':'high', 'Low':'low', 'Volume':'volume'}, axis=1)
                df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
                for row in df.iterrows():
                    row = row[1]
                    csv_writer.write_if_needed([row[0], row[1], row[2], row[3], row[4], row[5]], start_dt)
            else:
                kbars = self.api.kbars(
                    self.api.Contracts.Stocks[stock],
                    start=self.EQUITY_SINCE,
                    end=self.TODAY
                )
                df = pd.DataFrame({**kbars})
                df.ts = pd.to_datetime(df.ts)
                df = df.rename({'ts':'datetime', 'Open':'open', 'Close':'close', 'High':'high', 'Low':'low', 'Volume':'volume'}, axis=1)
                df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
                df.to_csv(file, index=False)

    def subscribe(self, contract, quote_type, intraday_odd=False):
        self.api.quote.subscribe(
            contract, # self.api.Contracts.Stocks["2330"]
            quote_type = quote_type,
            version = sj.constant.QuoteVersion.v1,
            intraday_odd = intraday_odd
        )