import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import numba as nb
import pandas_ta 
import os
import datetime
import json
from tabulate import tabulate
import sys
sys.path.append('../..')
import vectorbtpro as vbt
from vectorbtpro.portfolio.enums import SizeType
from src.utils import fu
from src.utils import plot_return_mdd
from src.strategy.BackTester import BackTester
from src.strategy.Analyzer import Analyzer
# from src.strategy.PositionSizer import PositionSizer
from src.strategy.MultiTester import MultiTester
from src.utils import plot_return_mdd,twinx_plot # as utils

import json
# config_f = open('..\configs\config.json')
# config = json.load(config_f)# %%

def get_data(coin):
    pair = f'{coin}USDT'
    df = pd.read_hdf(f'/Volumes/crypto_data/price_data/binance/1m/{pair}_PERPETUAL.h5')
    return df

class Strategy(BackTester):

    def __init__(self, df, configs, **kwargs):
        super().__init__(**kwargs)
        self.configs = configs
        self.freq = self.configs['freq']
        self.fee = self.configs['fee']
        self.df = self.resample_df(df=df, freq=self.freq)
        self._strategy_setting()
        self.indicator = pd.DataFrame()
    
    def resample_df(self,df,freq = '1h'):
        cols = ['open', 'high', 'low', 'close','volume']
        agg =  ['first','max',  'min', 'last', 'sum']
        df = df[cols]
        df = df.resample(freq).agg(dict(zip(cols,agg)))
        return df.dropna()
    
    def _strategy(self, df, side='both', **params):
        
        # params
        window_l = int(params['window_l'])
        window_s = int(params['window_s'])

        # df['log_rtn_sq'] = np.square(np.log(df['close']/df['close'].shift(1)))
        # df['RV'] = np.sqrt(df['log_rtn_sq'].rolling(window_l).sum())
        # df['RV_pctrank'] = df['RV'].rolling(window_l).rank(pct=True)   
        # rv_pct_EMA = df['RV_pctrank'].ewm(span=window_l, adjust=False).mean()
        # RV_filter = rv_pct_EMA > 20 and rv_pct_EMA < 80

        df['lema_1'] = df['close'].ewm(span=window_l, adjust=False).mean()
        df['lema_2'] = df['lema_1'].ewm(span=window_l, adjust=False).mean()
        df['lema_3'] = df['lema_2'].ewm(span=window_l, adjust=False).mean()
        df['tema_l'] = 3 * df['lema_1'] - 3 * df['lema_2'] + df['lema_3']
        
        long_entry = (df['close'].shift(1) < df['tema_l'].shift(1)) & (df['close'] > df['tema_l'])
        long_exit = (df['close'].shift(1) > df['tema_l'].shift(1)) & (df['close'] < df['tema_l']) 

        df['sema_1'] = df['close'].ewm(span=window_s, adjust=False).mean()
        df['sema_2'] = df['sema_1'].ewm(span=window_s, adjust=False).mean()
        df['sema_3'] = df['sema_2'].ewm(span=window_s, adjust=False).mean()
        df['tema_s'] = 3 * df['sema_1'] - 3 * df['sema_2'] + df['sema_3']

        short_entry = (df['close'].shift(1) > df['tema_s'].shift(1)) & (df['close'] < df['tema_s'])
        short_exit = (df['close'].shift(1) < df['tema_l'].shift(1)) & (df['close'] > df['tema_l'])

        if side == 'long':
            short_entry = False
            short_exit = False

        elif side == 'short':
            long_entry = False
            long_exit = False

        price = df['open'].shift(-self.lag)
        pf = vbt.Portfolio.from_signals(price, # type: ignore
                                        open = df['open'],
                                        high = df['high'],
                                        low  = df['low'],
                                        entries=long_entry,
                                        exits=long_exit,
                                        short_entries=short_entry,
                                        short_exits=short_exit,
                                        # sl_stop= 0.05,
                                        upon_opposite_entry='reverse'
                                        )
        return pf, params