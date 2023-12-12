import gc
import json
import pandas as pd
import numpy as np


class SectionCrosser:
    def __init__(self, ini, df):
        self.ini = ini
        self.df = df

    def cross_section_scale(self, df, usecols):
        """
        Featur Scaling by each group of pids
        (X - group mean) / group scale
        """
        data = df
        # drop id which not in k-means group
        # universe_pool = []
        # for g in group.keys():
        #     universe_pool += group[g]
        data[usecols].replace([np.inf, -np.inf], np.nan, inplace=True)
        # compute mean, max, min of each group
        g_mu = data.groupby("datetime")[usecols].mean()
        g_max = data.groupby("datetime")[usecols].max()
        g_min = data.groupby("datetime")[usecols].min()
        g_mu_list, g_max_list, g_min_list = [], [], []
        for _ in range(len(data["id"].unique())):
            g_mu_list.append(g_mu)
            g_max_list.append(g_max)
            g_min_list.append(g_min)
        df_g_mu = pd.concat(g_mu_list, axis=0).reset_index(drop=True)
        df_g_max = pd.concat(g_max_list, axis=0).reset_index(drop=True)
        df_g_min = pd.concat(g_min_list, axis=0).reset_index(drop=True)
        df_scale = df_g_max - df_g_min
        df_scale[df_g_max > df_scale] = df_g_max
        df_scale[-df_g_min > df_scale] = -df_g_min
        # feature scaling
        data[usecols] = (data[usecols] - df_g_mu) / df_scale
        return data

    def cross_section(self):
        data_dir = self.ini["Base"]["output_dir"]
        time_split = self.ini["Base"].getlist("time_split")
        time_split = list(map(str, time_split))

        with open("configs/column_dict.json", "r") as fp:
            train_config = json.load(fp)

        alphas = train_config["alpha"]
        signals = train_config["signal"]

        df = self.df
        # files = alphas + signals
        # print(files)
        # for file_name in files:
        #     raw_alpha = pd.read_csv(f'./output/{file_name}.csv', index_col='datetime')
        #     melt_df = raw_alpha.melt(ignore_index=False)
        #     temp = []
        #     for id in raw_alpha.columns[1:]: #datetime 跳過
        #         coin_alpha = melt_df[melt_df['variable'] == id]
        #         temp.append(coin_alpha)
        #     alpha_df = pd.concat(temp)
        #     alpha_df.columns = ['id'] + [file_name]
        #     df.append(alpha_df)

        # df = pd.concat(df, axis=1)
        # df = df.loc[:, ~df.columns.duplicated()]
        # df.index = pd.to_datetime(df.index)

        print(df)
        # --------------------------------- train test split --------------------------------
        train = df.loc[time_split[0] : time_split[1]]
        valid = df.loc[time_split[0] : time_split[1]]
        test = df.loc[time_split[0] : time_split[1]]
        # train = df[
        #     (df.index >= time_split[0]) & (df.index < time_split[1])
        # ]

        # valid = df[
        #     (df.index >= time_split[1]) & (df.index < time_split[2])
        # ]

        # test = df[
        #     (df.index >= time_split[2]) & (df.index < time_split[3])
        # ]

        print(
            "Train Period: {} ~ {}".format(
                train.index[0], train.index[-1]
            )
        )
        print(
            "Valid Period: {} ~ {}".format(
                valid.index[0], valid.index[-1]
            )
        )
        print(
            "Test Period:  {} ~ {}".format(
                test.index[0], test.index[-1]
            )
        )
        del df
        gc.collect()
        # ----------------------------- scaling by group and all ----------------------------
        # print("Cross-sectional scaling ...")
        # with open(
        #     "{}/{}/SumofSquareReturn10group.json".format(data_dir, time_split[2]), "r"
        # ) as config:
        #     group = json.load(config)
        # cross sectional scaling (by all)
        # train = self.cross_section_scale(train, usecols)
        # valid = self.cross_section_scale(valid, usecols)
        # if len(test) != 0:
        #     test = self.cross_section_scale(test, usecols)
        # ----------------------------- cluster get new features ----------------------------
        # print("Combine group features ...")
        # make agg mean features
        # mat1, mat2, mat3 = [], [], []
        # for i in group.keys():
        #     ind = group[i]
        #     # train
        #     newDf = train.loc[train["id"].isin(ind)][
        #         train_config["feature"] + ["datetime"]
        #     ]
        #     newDf = newDf.groupby(["datetime"]).agg(np.nanmean)
        #     newDf["group"] = "group{}".format(i)
        #     mat1.append(newDf)
        #     # valid
        #     newDf = valid.loc[valid["id"].isin(ind)][
        #         train_config["feature"] + ["datetime"]
        #     ]
        #     newDf = newDf.groupby(["datetime"]).agg(np.nanmean)
        #     newDf["group"] = "group{}".format(i)
        #     mat2.append(newDf)
        #     if len(test) != 0:
        #         # test
        #         newDf = test.loc[test["id"].isin(ind)][
        #             train_config["feature"] + ["datetime"]
        #         ]
        #         newDf = newDf.groupby(["datetime"]).agg(np.nanmean)
        #         newDf["group"] = "group{}".format(i)
        #         mat3.append(newDf)
        # mat1 = pd.concat(mat1)
        # mat1["datetime"] = mat1.index
        # mat2 = pd.concat(mat2)
        # mat2["datetime"] = mat2.index
        # if len(test) != 0:
        #     mat3 = pd.concat(mat3)
        #     mat3["datetime"] = mat3.index
        # -------------------------------- combine features ---------------------------------
        # 這個我覺得不用group，他前面把特徵縮放之後，不要kmeans了，所以這個大for迴圈就都不用，直接到下面saving就好
        # combine original features and agg mean features
        # train_mat, valid_mat, test_mat = [], [], []
        # for i in tqdm(group.keys()): 
        #     ind = group[i]
        #     # train
        #     train_group_mat = (
        #         mat1[mat1["group"] == "group{}".format(i)]
        #         .iloc[:, :-2]
        #         .reset_index(drop=True)
        #         .fillna(method="ffill")
        #     )
        #     train_group_mat.columns = [(i + "_mat") for i in train_group_mat.columns]
        #     # valid
        #     valid_group_mat = (
        #         mat2[mat2["group"] == "group{}".format(i)]
        #         .iloc[:, :-2]
        #         .reset_index(drop=True)
        #         .fillna(method="ffill")
        #     )
        #     valid_group_mat.columns = [(i + "_mat") for i in valid_group_mat.columns]
        #     if len(test) != 0:
        #         # test
        #         test_group_mat = (
        #             mat3[mat3["group"] == "group{}".format(i)]
        #             .iloc[:, :-2]
        #             .reset_index(drop=True)
        #             .fillna(method="ffill")
        #         )
        #         test_group_mat.columns = [(i + "_mat") for i in test_group_mat.columns]
        #     for pid in ind:
        #         # train
        #         temp = train[train["id"] == pid].reset_index(drop=True)
        #         temp[train_config["feature"]] = temp[train_config["feature"]].fillna(
        #             method="ffill"
        #         )
        #         train_mat.append(pd.concat([temp, train_group_mat], axis=1))
        #         # valid
        #         temp = valid[valid["id"] == pid].reset_index(drop=True)
        #         temp[train_config["feature"]] = temp[train_config["feature"]].fillna(
        #             method="ffill"
        #         )
        #         valid_mat.append(pd.concat([temp, valid_group_mat], axis=1))
        #         if len(test) != 0:
        #             # test
        #             temp = test[test["id"] == pid].reset_index(drop=True)
        #             temp[train_config["feature"]] = temp[
        #                 train_config["feature"]
        #             ].fillna(method="ffill")
        #             test_mat.append(pd.concat([temp, test_group_mat], axis=1))
        # del train, valid
        # gc.collect()
        # train = pd.concat(train_mat, axis=0)
        # valid = pd.concat(valid_mat, axis=0)
        # if len(test) != 0:
        #     test = pd.concat(test_mat, axis=0)
        # del train_mat, valid_mat
        # gc.collect()
        # ----------------------------------- saving data -----------------------------------
        print("# of inf value in trainset: ", train.isin([np.inf, -np.inf]).sum().sum())
        print("# of inf value in validset: ", valid.isin([np.inf, -np.inf]).sum().sum())
        print("# of inf value in testset:  ", test.isin([np.inf, -np.inf]).sum().sum())

        test.replace([np.inf, -np.inf], np.nan, inplace=True)
        train.replace([np.inf, -np.inf], np.nan, inplace=True)
        valid.replace([np.inf, -np.inf], np.nan, inplace=True)

        test = test.fillna(0)
        train = train.fillna(0)
        valid = valid.fillna(0)

        test.dropna(inplace=True)
        train.dropna(inplace=True)
        valid.dropna(inplace=True)

        test.to_csv(f"{data_dir}/test.csv")
        train.to_csv(f"{data_dir}/train.csv.gz", compression="gzip")
        valid.to_csv(f"{data_dir}/valid.csv.gz", compression="gzip")
        
        testx = test[alphas].to_numpy()
        trainx = train[alphas].to_numpy()
        validx = valid[alphas].to_numpy()

        np.save(data_dir + "/testx.npy", testx)
        np.save(data_dir + "/trainx.npy", trainx)
        np.save(data_dir + "/validx.npy", validx)

        for signal in signals:
            testy = test[signal].to_numpy()
            trainy = train[signal].to_numpy()
            validy = valid[signal].to_numpy()

            np.save(data_dir + "/testy_{}.npy".format(signal), testy)
            np.save(data_dir + "/trainy_{}.npy".format(signal), trainy)
            np.save(data_dir + "/validy_{}.npy".format(signal), validy)

        del trainx, trainy, validx, validy, train, valid
        gc.collect()
