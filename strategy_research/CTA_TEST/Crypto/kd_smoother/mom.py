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
    df = pd.read_hdf(f'/Users/johnsonhsiao/Desktop/data/{pair}_PERPETUAL.h5')
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
        window_l_k = int(params['window_l_k'])
        window_l_d = int(params['window_l_d'])
        window_s_k = int(params['window_s_k'])
        window_s_d = int(params['window_s_d'])

        # params
        window_k = int(params['window_k'])
        window_d = int(params['window_d'])

        df.ta.stoch(high='high', low='low', close='close', k=window_k, d=window_d, append=True)
          
        df['double_d'] = df[f'STOCHd_{window_k}_{window_d}_3'].ewm(span=window_d, adjust=False).mean()
        df['double_dd'] = df['double_d'].ewm(span=window_d, adjust=False).mean()

        long_entry = (df[f'STOCHd_{window_k}_{window_d}_3'] > df['double_dd']) & \
                    (df[f'STOCHd_{window_k}_{window_d}_3'].shift(1) < df['double_dd'].shift(1)) 
        long_exit = (df[f'STOCHd_{window_k}_{window_d}_3'] < df['double_d']) & \
                    (df[f'STOCHd_{window_k}_{window_d}_3'].shift(1) > df['double_d'].shift(1))

        short_entry = (df[f'STOCHd_{window_k}_{window_d}_3'] < df['double_dd']) & \
                    (df[f'STOCHd_{window_k}_{window_d}_3'].shift(1) > df['double_dd'].shift(1)) 
        short_exit = (df[f'STOCHd_{window_k}_{window_d}_3'] > df['double_d']) & \
                    (df[f'STOCHd_{window_k}_{window_d}_3'].shift(1) < df['double_d'].shift(1))


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
                                        sl_stop= np.nan/100,
                                        upon_opposite_entry='reverse'
                                        )
        return pf, params