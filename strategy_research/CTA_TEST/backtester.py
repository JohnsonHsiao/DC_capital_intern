import os 
import sys
import importlib
import time
import warnings
import pandas as pd
import gc
import traceback
import json
warnings.filterwarnings("ignore")

from src.strategy.MultiTester import MultiTester

strategy_path = os.path.join(sys.path[0], 'Crypto')

strategy_folders = [folder for folder in os.listdir(strategy_path) if os.path.isdir(os.path.join(strategy_path, folder))]

start = '2022-01-01'
end = '2023-05-01'
_list = ['ETH','BTC','BNB','SOL','MATIC',
               'XRP','DYDX','AVAX','LINK','GAS',
               'DOGE','ORDI','TRB','WLD','ADA',
               'OP','FIL','ZRX','LTC','RUNE','ATOM',
               'ARB','GMT','ETC','ARK','BCH','DOT',
               'LDO','SUI','GALA','CAKE',
               'APE','INJ','FTM','APT','YFI','OMG',
               'SEI','EOS','1000SHIB','NEAR',
               'MKR','CYBER','UNI',
               'BLUR','SUSHI','WAVES','MASK','MANA',
               'EGLD','AAVE','NEO','FET','TRX','GRT','ALGO','STX','XLM']

df_dict = {}
for coin in _list:
    print(f'loading {coin}...')
    try:
        pair = f'{coin}USDT'
        df = pd.read_hdf(f'Y:\\price_data\\binance\\1m\\{pair}_PERPETUAL.h5')
    except:
        # df = pd.read_hdf(f'/Volumes/crypto_data/price_data/binance/1m/{pair}_PERPETUAL.h5')
        df = pd.read_hdf(f'/Users/johnsonhsiao/Desktop/data/{pair}_PERPETUAL.h5')
    df_dict[coin] = df
    
print('done')
    
def get_data(df_dict, coin):
    return df_dict[coin]

with open(f'{strategy_path}/params_dict.json', 'r') as file:
    params_dict = json.load(file)
strategies = {}

for strategy_folder in ['bband_squeeze','weekend']:
    module_name = f'Crypto.{strategy_folder}.{strategy_folder}'
    print(strategy_folder)
    strategy_module = importlib.import_module(module_name)
    StrategyClass = getattr(strategy_module, 'Strategy')
    get_data_function = eval('get_data')
    params = params_dict[strategy_folder]
    sample_sets = [[start,end]]
    for freq in ['5T','15T','1h','4h']:
        config = {'freq':freq,'lag':1, 'fee': 0.0003, 'weekend_filter': False}
        for symbol in _list:
            if os.path.exists(f"{strategy_path}/{strategy_folder}/opt/{freq}/{symbol}"):
                continue
            else:
                print(symbol)
                try:
                    multi_test = MultiTester(
                        StrategyClass,
                        get_data_func=get_data_function,
                        params=params,
                        df_dict=df_dict,
                        config=config,
                        symbol_list=[symbol],
                        start=start,
                        end=end,
                        save_path = f'{strategy_path}/{strategy_folder}/'
                        )
                    all_params = multi_test.multi_params([symbol],sample_sets,direction='L/S')
                    trades, value_df = multi_test.multi_params_result(all_params)
                    del all_params
                    del trades
                    del value_df
                    gc.collect()
                except Exception as e:
                    print("An error occurred:", e)
                    pass