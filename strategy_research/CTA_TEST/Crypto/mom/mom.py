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
    try:
        pair = f'{coin}USDT'
        df = pd.read_hdf(f'/Volumes/crypto_data/price_data/binance/1m/{pair}_PERPETUAL.h5')
    except:
        # df = pd.read_hdf(f'/Users/johnsonhsiao/{pair}_PERPETUAL.h5')
        df = pd.read_hdf(f'Y:\\price_data\\binance\\1m\\{pair}_PERPETUAL.h5')
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
        # upper_bound = int(params['upper_bound'])
        # rv_rolling = int(params['rv_rolling'])

        # df['log_rtn_sq'] = np.square(np.log(df['close']/df['close'].shift(1)))
        # df['RV'] = np.sqrt(df['log_rtn_sq'].rolling(rv_rolling).sum())
        # df['RV_pctrank'] = df['RV'].rolling(rv_rolling).rank(pct=True)*100
        # rv_pct_MA = df['RV_pctrank'].rolling(72).mean()*100
        # RV_filter = (df['RV_pctrank'] > 100-upper_bound) & (df['RV_pctrank'] < upper_bound)

        df.ta.stoch(high='high', low='low', close='close', k=window_l_k, d=window_l_d, append=True)
        df.ta.stoch(high='high', low='low', close='close', k=window_s_k, d=window_s_d, append=True)

        df['double_l_d'] = df[f'STOCHd_{window_l_k}_{window_l_d}_3'].ewm(span=window_l_d, adjust=False).mean()
        df['double_s_d'] = df[f'STOCHd_{window_s_k}_{window_s_d}_3'].ewm(span=window_s_d, adjust=False).mean()
        df['double_l_dd'] = df['double_l_d'].ewm(span=window_l_d, adjust=False).mean()
        df['double_s_dd'] = df['double_s_d'].ewm(span=window_s_d, adjust=False).mean()

        long_entry = (df[f'STOCHd_{window_l_k}_{window_l_d}_3'] > df['double_l_dd']) & \
                    (df[f'STOCHd_{window_l_k}_{window_l_d}_3'].shift(1) < df['double_l_dd'].shift(1)) #& RV_filter
        long_exit = (df[f'STOCHd_{window_l_k}_{window_l_d}_3'] < df['double_l_d']) #| ~RV_filter

        short_entry = (df[f'STOCHd_{window_s_k}_{window_s_d}_3'] < df['double_s_dd']) & \
                    (df[f'STOCHd_{window_s_k}_{window_s_d}_3'].shift(1) > df['double_s_dd'].shift(1)) #& RV_filter
        short_exit = (df[f'STOCHd_{window_s_k}_{window_s_d}_3'] > df['double_s_d']) #| ~RV_filter
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
                                        # sl_stop= 0.1,
                                        upon_opposite_entry='reverse'
                                        )
        return pf, params




# window_entry_k = int(params['window_entry_k'])
#         window_entry_d = int(params['window_entry_d'])
#         window_exit_k = int(params['window_exit_k'])
#         window_exit_d = int(params['window_exit_d'])

#         df.ta.stoch(high='high', low='low', close='close', k=window_entry_k, d=window_entry_d, append=True)
#         df.ta.stoch(high='high', low='low', close='close', k=window_exit_k, d=window_exit_d, append=True)

#         df['double_entry_d'] = df[f'STOCHd_{window_entry_k}_{window_entry_d}_3'].ewm(span=window_entry_d, adjust=False).mean()
#         df['double_entry_dd'] = df['double_entry_d'].ewm(span=window_entry_d, adjust=False).mean()
#         df['double_exit_d'] = df[f'STOCHd_{window_exit_k}_{window_exit_d}_3'].ewm(span=window_exit_k, adjust=False).mean()
#         df['double_exit_dd'] = df['double_exit_d'].ewm(span=window_exit_k, adjust=False).mean()
        
#         long_entry = (df[f'double_exit_d'] > df['double_entry_dd']) & \
#                      (df[f'double_exit_d'].shift(1) < df['double_entry_dd'].shift(1)) 
#         long_exit = (df[f'STOCHd_{window_exit_k}_{window_exit_d}_3'] < df['double_exit_dd']) 

#         short_entry = (df[f'double_exit_d'] < df['double_entry_dd']) & \
#                       (df[f'double_exit_d'].shift(1) > df['double_entry_dd'].shift(1))
#         short_exit = (df[f'STOCHd_{window_exit_k}_{window_exit_d}_3'] > df['double_exit_dd']) 
