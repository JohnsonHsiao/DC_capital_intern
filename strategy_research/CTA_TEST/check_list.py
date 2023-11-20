import os 
import sys

strategy_path = os.path.join(sys.path[0], 'Crypto')
strategy_folders = [folder for folder in os.listdir(strategy_path) if os.path.isdir(os.path.join(strategy_path, folder))]

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

def check(strategy, freq):
    checking_list = os.listdir(f'{strategy_path}/{strategy}/opt/{freq}')
    for symbol in _list:
        if symbol not in checking_list:
            print(freq,symbol)
            
for freq in ['5T','15T','1h','4h']:
    for strategy in ['bband']:
        check(strategy, freq)
        