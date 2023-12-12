import json
import os
import gc
from tqdm import tqdm
import sys
import numpy as np
import pandas as pd
from datetime import timedelta, date, datetime

uperp_data = './data'
configs = './configs'

class FactorEvaluation:
    def __init__(self, df, ini, ic_mode):
        self.df = df
        self.ini = ini
        self.ic_mode = ic_mode
        with open(self.ini["Base"]["alpha_config"], "r") as config:
            self.alpha_config = json.load(config)
        
    def ic_cal(self):
        #correlation calculation function
        def normal_ic(group, target, alpha):
            return group[target].corr(group[alpha])
        def rank_ic(group, target, alpha):
            return group[target].rank().corr(group[alpha].rank(), method='spearman')
        # generated_alpha = self.alpha_config["feature"].keys()  
        generated_alpha = self.df.columns # 等全部都可用就用上面
        print(generated_alpha)
        target_list = [col for col in self.df.columns if 'return' in col]
        filtered_df = self.df.groupby(level=0).filter(lambda x: len(x) >= 3) #每日資料不能太少，不然算不了
        ic_table = pd.DataFrame()
        
        for target in target_list:
            for alpha in tqdm(generated_alpha, total=int(len(generated_alpha)), desc="Processing"):
                if self.ic_mode == 'normal':
                    correlation_per_time = filtered_df.groupby(level=0).apply(lambda group: normal_ic(group, target, alpha))
                else:
                    correlation_per_time = filtered_df.groupby(level=0).apply(lambda group: rank_ic(group, target, alpha))
                ic_table[alpha] = correlation_per_time
            ic_table.to_csv(f'ICIR_table/{target}_ic_table.csv')
        
    def ic_ir_report(self):
        # ir test function, i haven't write the threshold yet, u can modify it
        icir_table = {}
        for table_file in os.listdir('ICIR_table'):
            ic_table = pd.read_csv(f'ICIR_table/{table_file}')
            # generated_alpha = self.alpha_config["feature"].keys()
            generated_alpha = self.df.columns # 等全部都可用就用上面            
            icir_table[table_file] = {}
            for alpha in tqdm(generated_alpha, total=len(generated_alpha), desc="Processing"):
                ic_mean = ic_table[alpha].mean()
                print(alpha)
                print(ic_mean)
                ic_ir = ic_mean / ic_table[alpha].std()
                icir_table[table_file][alpha] = [ic_mean, ic_ir]
        with open('icir_result.json', 'w') as file:
            json.dump(icir_table, file)
            
    

