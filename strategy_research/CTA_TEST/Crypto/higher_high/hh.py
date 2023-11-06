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

        window_1 = int(params['window_1'])
        window_2 = int(params['window_2'])
        
        df['spread'] = df['high'] / df['low'] - 1
        df['spread_max'] = df['spread'].rolling(window_1).max().shift(1)
        df['range'] = df['spread_max'] < .05
       
        df['rolling_high'] = df['high'].rolling(window_1).max()
        df['rolling_low'] = df['low'].rolling(window_2).min()
        # 判斷是否突破過去N根K棒的最高和最低點
        df['break_high'] = df['close'] > df['rolling_high'].shift(1)
        df['break_low'] = df['close'] < df['rolling_low'].shift(1)

        df['rolling_volume'] = df['volume'].shift(1).rolling(window_1).max()
        df['abnormal_volume'] = df['volume'] > df['rolling_volume'] 

        long_entry = df['break_high'] & df['range'] & df['abnormal_volume']
        long_exit = df['range'] & (df['close'] < df['close'].shift(1))

        short_entry = df['break_low'] & df['range'] & df['abnormal_volume']
        short_exit = df['range'] & (df['close'] > df['close'].shift(1))

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
                                        # sl_stop=,
                                        upon_opposite_entry='reverse'
                                        )
        return pf, params