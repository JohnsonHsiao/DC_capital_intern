import os 
import sys
import importlib
import warnings
import json
warnings.filterwarnings("ignore")

from src.strategy.MultiTester import MultiTester

strategy_path = os.path.join(sys.path[0], 'Crypto')

strategy_folders = [folder for folder in os.listdir(strategy_path) if os.path.isdir(os.path.join(strategy_path, folder))]

start = '2022-01-01'
end = '2023-05-01'
symbol_list = ['ETH','BTC']

with open(f'{strategy_path}/params_dict.json', 'r') as file:
    params_dict = json.load(file)
strategies = {}
for strategy_folder in strategy_folders:
    module_name = f'Crypto.{strategy_folder}.{strategy_folder}'
    print(strategy_folder)
    try:
        strategy_module = importlib.import_module(module_name)
        StrategyClass = getattr(strategy_module, 'Strategy')
        get_data_function = getattr(strategy_module, 'get_data')
        
        sample_sets = [[start,end]]
        for freq in ['5T','15T','1h','4h']:
            config = {'freq':freq, 'lag':1, 'fee': 0.0003, 'weekend_filter': False}
            params = params_dict[strategy_folder]
            multi_test = MultiTester(
                StrategyClass,
                get_data_func=get_data_function,
                params=params,
                config=config,
                symbol_list=symbol_list,
                start=start,
                end=end
                )
            all_params = multi_test.multi_params(symbol_list,sample_sets,direction='L/S')
            trades, value_df = multi_test.multi_params_result(all_params)
            raise
    except (ImportError, AttributeError) as e:
        print(f"Error importing {module_name}: {e}")

