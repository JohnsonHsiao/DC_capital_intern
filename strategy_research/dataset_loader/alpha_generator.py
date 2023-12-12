import json
import pandas as pd
import os
import itertools
from tqdm import tqdm
from utils.GuoTaiAlpha191 import *
import datetime

class AlphaGenerator:
    def __init__(self, market, dataset, alphaConfig, time_split, output_dir):
        self.market = market
        self.dataset = dataset
        self.alphaConfig = alphaConfig
        self.ts = time_split
        self.output_dir = output_dir

    def run(self):
        if os.path.exists(self.output_dir) == False:
            os.makedirs(self.output_dir)

        if self.market == 'CRYPTO':
            self.generate_crypto_alpha()
        elif self.market == 'TW':
            self.generate_tw_alpha()

    def generate_crypto_alpha(self):
        # generate signal
        # pairs = self.dataset.df.columns.levels[0].to_list()
        target_config = self.alphaConfig["target"]
        feature_config = self.alphaConfig["feature"]
        intradayFeatureConfig = self.alphaConfig["intradayFeature"]
        # Calculate y
        column_dict = {}
        column_dict["signal"] = []
        column_dict["alpha"] = []
        # print(self.dataset.df)
        for l in target_config["length"]:
            signal_name = "{}_{}day_return".format(target_config["mode"], l)
            column_dict["signal"].append(signal_name)
            self.dataset.df[signal_name] = globals()[target_config["mode"]](self.dataset.df, l)
            
        
        # print(self.dataset.df.loc['2023-11-10 09:00:00' : '2023-11-10 11:00:00'])
            

            # df_list = []
            # for pair in pairs:
                # df_ret = globals()[target_config["mode"]](self.dataset.df[pair], l)
                # df_list.append(df_ret)

            # df_combine = pd.concat(df_list, axis=1)
            # df_combine.columns = self.dataset.df.columns.levels[0]
            # df_combine.to_csv(f"{self.output_dir}/{return_name}.csv")

        # -------------------------------- generate intraday Technical alphas --------------------------------
        # if os.path.isfile(
        #     f"/var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_intraday.csv"
        # ):
        #     print(
        #         f"\033[0;32m......../var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_intraday.csv already exists, please make sure the data inside are correct.\033[0m"
        #     )
        #     intraDayData = pd.read_csv(
        #         f"/var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_intraday.csv"
        #     )
        # else:
        #     pass
        #     # intraDayData = get_KLine(intraDaypids, validDateFromDataset)

        # intraDayData[["open", "high", "low", "close", "volume"]] = intraDayData[
        #     ["open", "high", "low", "close", "volume"]
        # ].astype(float)
        # intraDayData["Date"] = intraDayData["Date"].astype(str)
        # intraDayData["stock_id"] = (
        #     intraDayData["stock_id"].astype(str).replace("\.0", "", regex=True)
        # )
        # print("original intraDayData: ", intraDayData)

        # for alpha in tqdm(intradayFeatureConfig.keys()):
        #     df_alpha = globals()[alpha](
        #         intraDayData, validDateFromDataset, intraDaypids
        #     )
        #     df_alpha = df_alpha.fillna(method="ffill")
        #     df_alpha = df_alpha.pivot(index="Date", columns="stock_id")[alpha]
        #     df_alpha = df_alpha[df_alpha.index >= self.start]
        #     for pair in pairs:
        #         # print(alpha)
        #         # print(pair)
        #         # print(globals()[f'df_{pair}'])
        #         # print("length:", len(df_alpha[pair].values))
        #         # print(df_alpha[pair])
        #         globals()[f"df_{pair}"][alpha] = df_alpha[pair].values
        #         globals()[f"df_{pair}"]["Date"] = [i for i in df_alpha.index.to_list()]
        #         globals()[f"df_{pair}"] = globals()[f"df_{pair}"][
        #             globals()[f"df_{pair}"]["Date"] >= self.start
        #         ]

        # -------------------------------- generate daily Technical alphas --------------------------------
        for alpha, _ in tqdm(feature_config.items()):
            if alpha == 'alpha020':
                break
            # print(alpha)
            windows = [values for values in feature_config[alpha].values()]
            windows_list = [list(window_group) for window_group in zip(*windows)]
            if len(windows_list) == 0: # if this alpha don't need window
                windows_list.append([0])
            # print(windows)
            # print(windows_list)
            for window in windows_list:
                title = ""
                if window != [0]:
                    for i in window:
                        title += "_{}".format(i) # postfix of the alpha name
                column_dict["alpha"].append(alpha+title)
                if len(windows) == 0:
                    self.dataset.df[alpha+title] = globals()[alpha](self.dataset.df)
                else:
                    self.dataset.df[alpha+title] = globals()[alpha](self.dataset.df, *window)
                # print(alpha+title)
                self.dataset.df[alpha+title] = self.dataset.df[alpha+title].fillna(method="ffill")
        print(self.dataset.df)
        
        # self.dataset.df.to_csv(f"{self.output_dir}/data.csv")

        # for alpha, _ in tqdm(feature_config.items()):
        #     print(alpha)
        #     windows = [values for values in feature_config[alpha].values()]
        #     windows_list = [list(window_group) for window_group in zip(*windows)]

        #     if len(windows_list) == 0: # if this alpha don't need window
        #         windows_list.append([0])

        #     for window in windows_list:
        #         title = ""
        #         if window != [0]:
        #             for i in window:
        #                 title += "_{}".format(i) # postfix of the alpha name

        #         column_dict["alpha"].append(alpha+title)
        #         # Read alpha csv if exists
        #         # if os.path.isfile(f"{self.output_dir}/{alpha}{title}.csv"):
        #             # df_alpha = pd.read_csv(f"{self.output_dir}/{alpha}{title}.csv", index_col=0)
        #             # TODO: check if the alpha csv is correct or latest updated!
        #             # for pair in self.dataset.df.columns.levels[0]:
        #             #     self.dataset.df[pair, alpha+title] = df_alpha[pair].values
        #             # continue

        #         df_list = []
        #         for pair in self.dataset.df.columns.levels[0]:
        #             if len(windows) == 0:
        #                 df_alpha = globals()[alpha](self.dataset.df[pair])
        #             else:
        #                 df_alpha = globals()[alpha](self.dataset.df[pair], *window)

        #             df_alpha = df_alpha.fillna(method="ffill")
        #             # self.dataset.df[pair, alpha+title] = df_alpha.values # level[0] = pair, leve1[1] = alpha
        #             df_list.append(df_alpha)

        #         df_combine = pd.concat(df_list, axis=1)
        #         df_combine.columns = self.dataset.df.columns.levels[0]
        #         df_combine.to_csv(f"{self.output_dir}/{alpha}{title}.csv")

        with open(f"./configs/column_dict.json", "w") as f:
            json.dump(column_dict, f)
        print(column_dict)

    def generate_tw_alpha(self):
        # generate signal
        pids = self.dataset.df["open"].columns.to_list()
        config1 = self.alphaConfig["signal"]
        config2 = self.alphaConfig["feature"]
        intradayFeatureConfig = self.alphaConfig["intradayFeature"]
        if list(config1.keys())[0] == "SignalTimeCounter":
            for pid in pids:
                globals()[f"df_{pid}"] = pd.DataFrame()
                for l in config1["length"]:
                    df_ret = (
                        self.dataset.df["close"].shift(-l)
                        - self.dataset.df["open"].shift(-1)
                    ) / self.dataset.df["open"].shift(-1)
                    globals()[f"df_{pid}"][
                        "{}_{}day_return".format(config1["mode"], l)
                    ] = df_ret[pid].values
                globals()[f"df_{pid}"]["Date"] = [
                    i for i in self.dataset.df.index.to_list()
                ]
                globals()[f"df_{pid}"] = globals()[f"df_{pid}"][
                    globals()[f"df_{pid}"]["Date"] >= self.start
                ]

        # -------------------------------- generate intraday Technical alphas --------------------------------
        if os.path.isfile(
            f"/var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_TXF.csv"
        ):
            print(
                f"\033[0;32m......../var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_TXF.csv already exists, please make sure the data inside are correct.\033[0m"
            )
            TXF = pd.read_csv(
                f"/var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_TXF.csv"
            )
        else:
            TXF = self.get_KLine(["TXF"], self.ts)

        TXF[["open", "high", "low", "close", "volume"]] = TXF[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)
        TXF["Date"] = TXF["Date"].astype(str).replace("\.0", "", regex=True)
        startDate = TXF["Date"].values[0]
        validDateFromDataset = sorted(
            [i for i in self.dataset.df.index.to_list() if i >= startDate]
        )
        intraDaypids = pids + ["TXF"]

        if os.path.isfile(
            f"/var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_intraday.csv"
        ):
            print(
                f"\033[0;32m......../var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_intraday.csv already exists, please make sure the data inside are correct.\033[0m"
            )
            intraDayData = pd.read_csv(
                f"/var/files/AlphaPortfolio/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_intraday.csv"
            )
        else:
            intraDayData = get_KLine(intraDaypids, validDateFromDataset)
        intraDayData[["open", "high", "low", "close", "volume"]] = intraDayData[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)
        intraDayData["Date"] = intraDayData["Date"].astype(str)
        intraDayData["stock_id"] = (
            intraDayData["stock_id"].astype(str).replace("\.0", "", regex=True)
        )
        print("original intraDayData: ", intraDayData)

        for alpha in tqdm(intradayFeatureConfig.keys()):
            df_alpha = globals()[alpha](
                intraDayData, validDateFromDataset, intraDaypids
            )
            df_alpha = df_alpha.fillna(method="ffill")
            df_alpha = df_alpha.pivot(index="Date", columns="stock_id")[alpha]
            df_alpha = df_alpha[df_alpha.index >= self.start]
            for pid in pids:
                # print(alpha)
                # print(pid)
                # print(globals()[f'df_{pid}'])
                # print("length:", len(df_alpha[pid].values))
                # print(df_alpha[pid])
                globals()[f"df_{pid}"][alpha] = df_alpha[pid].values
                globals()[f"df_{pid}"]["Date"] = [i for i in df_alpha.index.to_list()]
                globals()[f"df_{pid}"] = globals()[f"df_{pid}"][
                    globals()[f"df_{pid}"]["Date"] >= self.start
                ]

        # -------------------------------- generate daily Technical alphas --------------------------------
        for alpha in tqdm(config2.keys()):
            windows = config2[alpha]
            if len(windows) == 0:
                df_alpha = globals()[alpha](self.dataset)
                df_alpha = df_alpha.fillna(method="ffill")
                df_alpha = df_alpha[df_alpha.index >= self.start]
                for pid in pids:
                    globals()[f"df_{pid}"][alpha] = df_alpha[pid].values
            else:
                for i in range(len(windows["window1"])):
                    win = []
                    alpha_name = alpha
                    for j in windows:
                        alpha_name += "_{}".format(windows[j][i])
                        win.append(windows[j][i])
                    df_alpha = globals()[alpha](self.dataset, *win)
                    # print('df_alpha: ', df_alpha)
                    df_alpha = df_alpha.fillna(method="ffill")
                    df_alpha = df_alpha[df_alpha.index >= self.start]
                    for pid in pids:
                        # print("df_alpha values: \n\n", df_alpha[pid].values)
                        # print("type: ", type(df_alpha[pid].values))
                        globals()[f"df_{pid}"][alpha_name] = df_alpha[pid].values

        # combine each stock data
        df_list = []
        for pid in pids:
            globals()[f"df_{pid}"]["stock_id"] = pid
            df_list.append(globals()[f"df_{pid}"])
        df_combine = pd.concat(df_list, axis=0)
        print(
            f"Output dataset csv to {self.output_dir}/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_data.csv.gz"
        )
        df_combine.to_csv(
            f"{self.output_dir}/{self.ts[0]}_{self.ts[1]}_{self.ts[2]}_{self.ts[3]}_data.csv.gz",
            index=False,
            compression="gzip",
        )