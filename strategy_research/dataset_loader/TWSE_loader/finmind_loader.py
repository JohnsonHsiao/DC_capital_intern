import requests
import pandas as pd

"""
API Document
https://finmind.github.io/tutor/TaiwanMarket/DataList/
"""

class FinmindLoader():
    
    def __init__(self):
        self.token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMi0xMi0yMiAxNDoyODozMiIsInVzZXJfaWQiOiJrZXZpbnlpbjkiLCJpcCI6IjIyMC4xMzAuMTIyLjE2NSJ9.DCveXYzBtljW4bjYOVCiZJQyJVe_zt-9dvaUWJHzdPg"

    def checkAPIRate(self):
        url = "https://api.web.finmindtrade.com/v2/user_info"
        payload = {
            "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMi0xMi0yMiAxNDoyODozMiIsInVzZXJfaWQiOiJrZXZpbnlpbjkiLCJpcCI6IjIyMC4xMzAuMTIyLjE2NSJ9.DCveXYzBtljW4bjYOVCiZJQyJVe_zt-9dvaUWJHzdPg",
        }
        resp = requests.get(url, params=payload)
        print("使用次數: {}/{}.".format(resp.json()["user_count"], resp.json()["api_request_limit"]))  # 使用次數

    def get_tw_stock_info(self):
        url = 'https://api.finmindtrade.com/api/v4/data'
        parameter = {
            "dataset": "TaiwanStockInfo"
        }
        data = requests.get(url, params=parameter)
        data = data.json()
        data = pd.DataFrame(data['data'])
        return data
    
    def get_tw_stock_daily_price(self, id, strat_date, end_date, token=False):
        url = 'https://api.finmindtrade.com/api/v4/data'
        parameter = {
            "dataset": "TaiwanStockPrice",
            "data_id": id,
            "start_date": strat_date,
            "end_date": end_date,
            "token": self.token if token else "", # 參考登入，獲取金鑰
        }
        data = requests.get(url, params=parameter)
        data = data.json()
        data = pd.DataFrame(data['data'])
        return data

    def get_us_stock_1min_data(self):
        data_url = 'https://api.finmindtrade.com/api/v4/data?'
        payload = {
            "dataset": "USStockPriceMinute",
            "data_id": "^IXIC",
            "start_date": "2022-12-21",
            "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMi0xMi0yMiAxNDoyODozMiIsInVzZXJfaWQiOiJrZXZpbnlpbjkiLCJpcCI6IjIyMC4xMzAuMTIyLjE2NSJ9.DCveXYzBtljW4bjYOVCiZJQyJVe_zt-9dvaUWJHzdPg",
        }
        resp = requests.get(data_url, params=payload)
        data = resp.json()
        data = pd.DataFrame(data['data'])
        return data


    def get_historical_tick_data(self, data_id: str, token=False): # 台灣股價歷史逐筆資料表 (資料量過大，單次請求只提供一天資料)
        data_url = 'https://api.finmindtrade.com/api/v4/data?'
        payload = {
            "dataset": "TaiwanStockPriceTick",
            "data_id": data_id,
            "start_date": "2022-12-23",
            "token": self.token if token else "", # 參考登入，獲取金鑰
        }
        resp = requests.get(data_url, params=payload)
        data = resp.json()
        data = pd.DataFrame(data['data'])
        return data

    def get_taiwan_stock_dividend_result(self, id: str, start_date: str, token=False):
        url = "https://api.finmindtrade.com/api/v4/data"
        parameter = {
            "dataset": "TaiwanStockDividendResult",
            "data_id": id,
            "start_date": start_date,
            "token": self.token if token else "", # 參考登入，獲取金鑰
        }
        data = requests.get(url, params=parameter)
        data = data.json()
        data = pd.DataFrame(data['data'])
        return data
    
    def get_taiwan_stock_dividend_policy(self, id: str, start_date: str, token=False):
        url = "https://api.finmindtrade.com/api/v4/data"
        parameter = {
            "dataset": "TaiwanStockDividend",
            "data_id": id,
            "start_date": start_date,
            "token": self.token if token else "", # 參考登入，獲取金鑰
        }
        data = requests.get(url, params=parameter)
        data = data.json()
        data = pd.DataFrame(data['data'])
        return data

    def get_taiwan_stock_margin_purchase_short_sale(self, id: str, start_date: str, token=False):
        url = "https://api.finmindtrade.com/api/v4/data"
        parameter = {
            "dataset": "TaiwanStockMarginPurchaseShortSale",
            "data_id": id,
            "start_date": start_date,
            "token": self.token if token else "", # 參考登入，獲取金鑰
        }
        data = requests.get(url, params=parameter)
        data = data.json()
        data = pd.DataFrame(data['data'])
        return data
        
    def get_tw_stock_price(self, id: str, start_date: str, end_date: str, token=False):
        url = "https://api.finmindtrade.com/api/v4/data"
        parameter = {
            "dataset": "TaiwanStockPrice",
            "data_id": id,
            "start_date": start_date,
            "end_date": end_date,
            "token": self.token if token else "", # 參考登入，獲取金鑰
        }
        resp = requests.get(url, params=parameter)
        data = resp.json()
        data = pd.DataFrame(data["data"])
        return data