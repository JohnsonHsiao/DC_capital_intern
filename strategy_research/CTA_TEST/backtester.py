import os 
import sys
import importlib
import time
import warnings
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
               '1000PEPE','LDO','SUI','GALA','CAKE',
               'APE','INJ','FTM','APT','YFI','OMG',
               'SEI','EOS','1000SHIB','NEAR','STORJ',
               '1000FLOKI','MKR','CYBER','UNI','STRAX',
               'BLUR','SUSHI','WAVES','MASK','MANA',
               'EGLD','AAVE','NEO','FET','TRX','GRT','ALGO','STX','XLM']

with open(f'{strategy_path}/params_dict.json', 'r') as file:
    params_dict = json.load(file)
strategies = {}

for strategy_folder in ['donchian_ma','keltner','weekend']:
    module_name = f'Crypto.{strategy_folder}.{strategy_folder}'
    print(strategy_folder)
    strategy_module = importlib.import_module(module_name)
    StrategyClass = getattr(strategy_module, 'Strategy')
    get_data_function = getattr(strategy_module, 'get_data')
    params = params_dict[strategy_folder]
    sample_sets = [[start,end]]
    for freq in ['15T','1h']:
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
                        config=config,
                        symbol_list=[symbol],
                        start=start,
                        end=end
                        )
                    print([symbol])
                    all_params = multi_test.multi_params([symbol],sample_sets,direction='L/S')
                    trades, value_df = multi_test.multi_params_result(all_params)
                    del all_params
                    del trades
                    del value_df
                    gc.collect()
                except Exception as e:
                    print("An error occurred:", e)
                    traceback.print_exc()
                    time.sleep(10)

