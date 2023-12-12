"""
datasetGenerator for fetching price data and calculcating the alpha.
"""
import json
import os
import gc
import warnings
from tqdm import tqdm
import numpy as np
import pandas as pd
from datetime import timedelta, date
from sklearn.cluster import KMeans

from .alpha_generator import AlphaGenerator
from .Utility import TimerWatch
from .binance_loader import BinanceLoader
from utils.intradayAlpha import *

warnings.filterwarnings("ignore")

class TWDataset:
    def __init__(
        self,
        usecols,
        start,
        end,
        top_mktv=False,
        universe_pool=False,
        data_sheet="日收盤還原表排行",
        rm=False,
        addtwa=False,
    ):
        self.usecols = usecols
        # set renamecolumn
        self.renamecol = {}
        self.rename_dict = {
            "開盤價": "Open",
            "最高價": "High",
            "最低價": "Low",
            "收盤價": "Close",
            "均價": "VWAP",
            "成交量": "Volume",
            "成交金額(千)": "Amount",
            "總市值(億)": "MktValue",
            "本益比": "PE",
            "股價淨值比": "PB",
            "週轉率(%)": "TO",
        }
        for col in [i for i in self.usecols if i not in ["日期", "股票代號"]]:
            self.renamecol[col] = self.rename_dict[col]
        self.start = start
        self.end = end
        self.addtwa = addtwa
        self.top_mktv = top_mktv
        self.universe_pool = universe_pool
        self.data_sheet = data_sheet
        self.df = self.__load_data()
        if rm == True:
            self.df_rm = self.load_rm()

    def __load_data(self):
        def str_to_float(x):
            try:
                return float(x)
            except:
                return np.nan

        def df_str_to_float(df, cols):
            for col in cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: str_to_float(x))

        def daterange(date1, date2):
            for n in range(int((date2 - date1).days) + 1):
                yield date1 + timedelta(n)

        print("Loading stock data from Cmoney ..........")
        df_list = []
        start_dt = date(int(self.start[:4]), int(self.start[4:6]), int(self.start[6:]))
        start_dt = start_dt - timedelta(days=150)  # 往前推150天: 因部分指標需過去一段時間的資料
        end_dt = date(int(self.end[:4]), int(self.end[4:6]), int(self.end[6:]))
        if self.data_sheet == "日收盤表排行":
            for dt in daterange(start_dt, end_dt):
                day = dt.strftime("%Y%m%d")
                temp1 = PX.Mul_Data(
                    self.data_sheet, "D", day, ps="<CM代號,1>"
                )  # 代號1: 上市 / 代號2: 上櫃
                temp2 = PX.Mul_Data(
                    self.data_sheet, "D", day, ps="<CM代號,2>"
                )  # 代號1: 上市 / 代號2: 上櫃
                if len(temp1) != 0:
                    df_list.append(temp1[self.usecols])
                    df_list.append(temp2[self.usecols])
                    # print('Loading data for ', day)
        elif self.data_sheet == "日收盤還原表排行":
            for dt in daterange(start_dt, end_dt):
                day = dt.strftime("%Y%m%d")
                # ------------------------------- 上市 -------------------------------
                temp1 = PX.Mul_Data(
                    self.data_sheet, "D", day, ps="<CM代號,1>"
                )  # 代號1: 上市 / 代號2: 上櫃
                df_str_to_float(temp1, self.renamecol.keys())
                temp1_real = PX.Mul_Data(
                    "日收盤表排行", "D", day, ps="<CM代號,1>", colist="開盤價,均價,股票代號"
                )
                df_str_to_float(temp1_real, self.renamecol.keys())
                # 調整除權息
                temp1["均價"] = temp1_real["均價"]
                temp1["adj"] = temp1["開盤價"] / temp1_real["開盤價"]
                for col in ["成交量", "均價", "總市值(億)", "本益比", "股價淨值比"]:
                    temp1[col] = temp1[col] * temp1["adj"]
                # ------------------------------- 上櫃 -------------------------------
                temp2 = PX.Mul_Data(
                    self.data_sheet, "D", day, ps="<CM代號,2>"
                )  # 代號1: 上市 / 代號2: 上櫃
                df_str_to_float(temp2, self.renamecol.keys())
                temp2_real = PX.Mul_Data(
                    "日收盤表排行", "D", day, ps="<CM代號,2>", colist="開盤價,均價,股票代號"
                )
                df_str_to_float(temp2_real, self.renamecol.keys())
                # 調整除權息
                temp2["均價"] = temp2_real["均價"]
                temp2["adj"] = temp2["開盤價"] / temp2_real["開盤價"]
                for col in ["成交量", "均價", "總市值(億)", "本益比", "股價淨值比"]:
                    temp2[col] = temp2[col] * temp2["adj"]
                # ------------------------------- 指數 -------------------------------
                if self.addtwa:
                    temp3 = PX.Mul_Data("日收盤表排行", "D", day, ps="<CM代號,指數>")  # 代號指數
                    df_str_to_float(temp3, self.renamecol.keys())
                if len(temp1) != 0:
                    df_list.append(temp1[self.usecols])
                    df_list.append(temp2[self.usecols])
                    if self.addtwa:
                        df_list.append(temp3[self.usecols])
                    # print('Loading data for ', day)
        # combine dataframe
        df = pd.concat(df_list, axis=0)
        for col in self.renamecol.keys():
            df[col] = df[col].apply(lambda x: str_to_float(x))
        df = df.reset_index(drop=True)
        return self.preprocess(df)

    def load_rm(self):
        def str_to_float(x):
            try:
                return float(x)
            except:
                return np.nan

        def df_str_to_float(df, cols):
            for col in cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: str_to_float(x))

        def daterange(date1, date2):
            for n in range(int((date2 - date1).days) + 1):
                yield date1 + timedelta(n)

        print("Loading Rm data from Cmoney ..........")
        # load Rm data
        df_list = []
        start_dt = date(int(self.start[:4]), int(self.start[4:6]), int(self.start[6:]))
        start_dt = start_dt - timedelta(days=150)
        end_dt = date(int(self.end[:4]), int(self.end[4:6]), int(self.end[6:]))
        for dt in daterange(start_dt, end_dt):
            day = dt.strftime("%Y%m%d")
            # print('Loading data for ', day)
            temp1 = PX.Mul_Data("日收盤表排行", "D", day, ps="<CM代號,指數>")
            temp1 = temp1[temp1["股票代號"] == "TWA02"][["日期", "收盤價"]]  # TWA02: 報酬指數
            df_list.append(temp1)
        df = pd.concat(df_list, axis=0)
        df.index = df["日期"]
        df = df.iloc[:, 1:]
        df_str_to_float(df, df.columns)
        return df

    def preprocess(self, df):
        # select universe pool
        if self.top_mktv:
            print(
                "Select stock pool top {} market value ..........".format(
                    self.top_mktv[0]
                )
            )
            latest_day = self.top_mktv[1]
            temp = df[df["日期"] <= latest_day]
            latest_day = sorted(temp["日期"].unique().tolist())[-1]
            temp = df[df["日期"] == latest_day]
            print("Latest day: ", latest_day)
            universe = temp.sort_values("總市值(億)", ascending=False)["股票代號"].tolist()[
                : self.top_mktv[0]
            ]
            df = df[df["股票代號"].isin(universe)].reset_index(drop=True)
        elif self.universe_pool:
            print("Select stock pool by given universe pool ..........")
            df = df[df["股票代號"].isin(self.universe_pool)].reset_index(drop=True)

        # combine each pid by multi-index
        print("Combine each pid by multi-index ..........")
        df.rename(columns=self.renamecol, inplace=True)
        df = df.pivot(index="日期", columns="股票代號")
        return df

class CryptoDataset:
    def __init__(
        self,
        start,
        end,
        top_mktv=None,
        training_pair_list=None
    ):
        self.start = start
        self.end = end
        self.top_mktv = top_mktv
        self.training_pair_list = training_pair_list
        self.df = None
        self.binance_loader = BinanceLoader()
        
        self.load_data()

    def load_data(self):

        print("Loading data from Binance ..........")

        start_dt = date(int(self.start[:4]), int(self.start[4:6]), int(self.start[6:]))
        start_dt = start_dt - timedelta(days=150)  # 往前推150天: 因部分指標需過去一段時間的資料
        end_dt = date(int(self.end[:4]), int(self.end[4:6]), int(self.end[6:]))

        # self.binance_loader.download(timeframe="1h", type="UPERP", training_pair_list=self.training_pair_list)

        self.df = self.binance_loader.uperp_data_import(timeframe="1h", training_pair_list=self.training_pair_list)
        
        # self.filterByVolume(df)

    def filterByVolume(self, df):
        # select universe pool
        if self.top_mktv:
            print(
                "Select stock pool top {} market value ..........".format(
                    self.top_mktv[0]
                )
            )
            latest_day = self.top_mktv[1]
            temp = df[df["datetime"] <= latest_day]
            latest_day = sorted(temp["datetime"].unique().tolist())[-1]
            temp = df[df["datetime"] == latest_day]
            print("Latest day: ", latest_day)
            universe = temp.sort_values("mktValue(USDT)", ascending=False)["id"].tolist()[
                : self.top_mktv[0]
            ]
            df = df[df["id"].isin(universe)].reset_index(drop=True)

        # combine each pid by multi-index
        print("Combine each pid by multi-index ..........")
        df = df.pivot(index="datetime", columns="id")
        self.df = df

class DatasetGenerator:
    """
    DatasetGenerator class
    """

    def __init__(self, ini):
        self.ini = ini
        self.market = self.ini["Base"]["market"]
        self.output_dir = self.ini["Base"]["output_dir"]
        self.time_split = list(map(str, self.ini["Base"].getlist("time_split")))
        self.start_date = self.time_split[0]
        self.end_date = self.time_split[-1]
        self.training_pair_list = self.ini["Base"].getlist("training_pair_list")
        with open(self.ini["Base"]["alpha_config"], "r") as config:
            self.alpha_config = json.load(config)

    def generate_dataset(self):
        """
        fetching price data and calculcating the alpha.
        """
        if self.market == 'CRYPTO':
            df = self.generate_crypto_dataset()
        elif self.market == 'TW':
            self.generate_tw_dataset()
        return df
    def generate_crypto_dataset(self):
        if os.path.exists(
            f"{self.output_dir}/{self.market}_{self.time_split[0]}_{self.time_split[1]}_{self.time_split[2]}_{self.time_split[3]}_data.csv.gz"
        ):
            df = pd.read_csv(
                f"{self.output_dir}/{self.market}_{self.time_split[0]}_{self.time_split[1]}_{self.time_split[2]}_{self.time_split[3]}_data.csv.gz"
            )
            print(
                f"\033[0;32m........{self.output_dir}/{self.market}_{self.time_split[0]}_{self.time_split[1]}_{self.time_split[2]}_{self.time_split[3]}_data.csv.gz already exists, please make sure the data inside are correct.\033[0m"
            )
            return df

        print(f"\033[0;32m........ Start generate crypto dataset!\033[0m")
        # select top N market value company as our universe pool
        # dataset = CryptoDataset(
        #     self.start_date,
        #     self.end_date,
        #     top_mktv=(int(self.ini["Base"]["top_N"]), self.end_date),
        # )
        # universe_pool = dataset.df["MktValue"].columns.to_list()

        # print("# of stock pool: ", len(universe_pool))

        # generate raw dataset: Open, High, Low, Close...
        dataset = CryptoDataset(
            self.start_date,
            self.end_date,
            training_pair_list=self.training_pair_list,
        )

        alphaGenerator = AlphaGenerator(
            self.market, dataset, self.alpha_config, self.time_split, self.output_dir
        )

        # Generate alpha and signal
        with TimerWatch(
            "\033[0;32m.............generate and combine Technical alphas ...\033[0m"
        ) as _:
            alphaGenerator.run()

        self.df = alphaGenerator.dataset.df 
        return self.df

        # # Cluster similar companys into N group
        # print(dataset.df)
        # for pair in self.dataset.df.columns.levels[0]:
        #     df_ret = dataset.df[pair, "close"].pct_change()
        #     df_ret = (df_ret**2).rolling(20).sum()
        # df_ret = df_ret[
        #     (df_ret.index >= self.time_split[0]) & (df_ret.index < self.time_split[1])
        # ]
        # # Kmeans clustering
        # n_c = int(self.ini["Base"]["N_group"])
        # kmeans = KMeans(n_clusters=n_c, random_state=123).fit(df_ret.corr().values)
        # # combine similar pids
        # pids = df_ret.columns.tolist()
        # print("length of pids after Kmeans clustering: ", len(pids))
        # group = {}
        # for i, v in enumerate(kmeans.labels_):
        #     if str(v) not in group:
        #         group[str(v)] = [pids[i]]
        #     else:
        #         group[str(v)].append(pids[i])
        # # save group
        # if os.path.exists("{}/{}".format(self.output_dir, self.time_split[2])) == False:
        #     os.makedirs("{}/{}".format(self.output_dir, self.time_split[2]))
        # with open(
        #     "{}/{}/SumofSquareReturn10group.json".format(self.output_dir, self.time_split[2]), "w" # why 10 groups?
        # ) as fp:
        #     json.dump(group, fp)
        

    def generate_tw_dataset(self):
        if os.path.exists(
            f"{self.output_dir}/{self.market}_{self.time_split[0]}_{self.time_split[1]}_{self.time_split[2]}_{self.time_split[3]}_data.csv.gz"
        ):
            df = pd.read_csv(
                f"{self.output_dir}/{self.market}_{self.time_split[0]}_{self.time_split[1]}_{self.time_split[2]}_{self.time_split[3]}_data.csv.gz"
            )
            print(
                f"\033[0;32m........{self.output_dir}/{self.market}_{self.time_split[0]}_{self.time_split[1]}_{self.time_split[2]}_{self.time_split[3]}_data.csv.gz already exists, please make sure the data inside are correct.\033[0m"
            )
            return df

        # select top N market value company as our universe pool
        dataset = TWDataset(
            ["date", "id", "marketValue"],
            self.start_date,
            self.end_date,
            top_mktv=(int(self.ini["Base"]["top_N"]), self.end_date),
            data_sheet="日收盤表排行",
        )
        universe_pool = dataset.df["MktValue"].columns.to_list()
        if self.ini["Base"]["add_TWSE"]:
            universe_pool.append("TWA00")
        print("# of stock pool: ", len(universe_pool))

        # generate raw dataset: Open, High, Low, Close...
        dataset = TWDataset(
            [
                "日期",
                "股票代號",
                "開盤價",
                "最高價",
                "最低價",
                "收盤價",
                "均價",
                "成交量",
                "成交金額(千)",
                "總市值(億)",
                "本益比",
                "股價淨值比",
                "週轉率(%)",
            ],
            self.start_date,
            self.end_date,
            universe_pool=universe_pool,
            addtwa=True,
        )
        print("length of universe_pool: ", len(universe_pool))

        alphaGenerator = AlphaGenerator(
            self.ini, dataset, self.alpha_config, self.start_date, self.output_dir
        )

        # generate Alpha and Signal
        with TimerWatch(
            "\033[0;32m.............generate and combine Technical alphas ...\033[0m"
        ) as _:
            alphaGenerator.run()

        # # Cluster similar companys into N group
        # df_ret = dataset.df["Close"].pct_change()
        # df_ret = (df_ret**2).rolling(20).sum()
        # df_ret = df_ret[
        #     (df_ret.index >= self.time_split[0]) & (df_ret.index < self.time_split[1])
        # ]
        # # select pids which exist at the begining
        # universe_pool = df_ret.iloc[0][(df_ret.iloc[0].isna() == False)].index.tolist()
        # df_ret = df_ret[universe_pool]
        # # Kmeans clustering
        # n_c = int(self.ini["Base"]["N_group"])
        # kmeans = KMeans(n_clusters=n_c, random_state=123).fit(df_ret.corr().values)
        # # combine similar pids
        # pids = df_ret.columns.tolist()
        # print("length of pids after Kmeans clustering: ", len(pids))
        # group = {}
        # for i, v in enumerate(kmeans.labels_):
        #     if str(v) not in group:
        #         group[str(v)] = [pids[i]]
        #     else:
        #         group[str(v)].append(pids[i])
        # # save group
        # if os.path.exists("{}/{}".format(self.output_dir, self.time_split[2])) == False:
        #     os.makedirs("{}/{}".format(self.output_dir, self.time_split[2]))
        # with open(
        #     "{}/{}/SumofSquareReturn10group.json".format(self.output_dir, self.time_split[2]), "w"
        # ) as fp:
        #     json.dump(group, fp)