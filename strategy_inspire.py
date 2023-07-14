#%%
import pandas as pd
import matplotlib.pyplot as plt


bitget_df = pd.read_csv('/Users/johnsonhsiao/DC_capital/data/bitget/1m_BTC.csv')
bitget = bitget_df[:-2]
bybit = pd.read_csv('/Users/johnsonhsiao/DC_capital/data/bybit/1m_BTC.csv')
spread = ((bybit['close'] - bitget['close'].astype(float)) / ((bybit['close'] + bitget['close'].astype(float)) / 2)) * 100

# %%

data = pd.DataFrame()
data['spread'] = spread
data['bitget'] = bitget['close'].astype(float)
data['bybit'] = bybit['close']


#%%

entry_threshold = 0.06
cost = 0.065
exp_earn = 0.01
exit_threshold = cost + exp_earn
unit_size = 1
unit = 1000
possibility = 1

positions = 0
entry_prices = []
avg_entry_price = 0
trade_records = []
cash = 100000  
cash_flow = []
pnl = 0

for i in range(len(data)):
    price = data['spread'].iloc[i]
    # bybit = data['bybit'].iloc[i]
    # bitget = data['bitget'].iloc[i]

    if price > entry_threshold and positions <= 0 and cash > 0:
        positions -= unit_size # - 代表組空bybit做多bitget
        cash -= unit_size * unit
        pnl = 0
        cash_flow.append({'cash':cash, 'pnl':pnl, 'position': abs(positions) * 1000})
        entry_prices.append(price)
        avg_entry_price = sum(entry_prices) / len(entry_prices)
        trade_records.append({'time': i, 'spread': price, 'avg_spread': avg_entry_price, 'direction': 'short'})

    elif price < -entry_threshold and positions >= 0 and cash > 0:
        positions += unit_size
        cash -= unit_size * unit
        pnl = 0
        cash_flow.append({'cash':cash, 'pnl':pnl, 'position': abs(positions) * 1000})
        entry_prices.append(price)
        avg_entry_price = sum(entry_prices) / len(entry_prices)
        trade_records.append({'time': i, 'spread': price, 'avg_spread': avg_entry_price, 'direction': 'long'})

    elif avg_entry_price - price > exit_threshold and positions < 0:
        positions += unit_size
        pnl = avg_entry_price - price - cost
        cash += unit_size * unit + unit * pnl * 0.01
        cash_flow.append({'cash':cash, 'pnl':pnl, 'position': abs(positions) * 1000})
        avg_entry_price = sum(entry_prices) - price / abs(positions)
        trade_records.append({'time': i, 'spread': price, 'avg_spread': avg_entry_price, 'direction': 'close short'})
        # pnl.append(positions * (avg_entry_price - price))  # 计算做空交易的P&L
        # cash += abs(positions) * (avg_entry_price - price)  # 更新账户现金

    elif price - avg_entry_price < exit_threshold and positions > 0:
        positions -= unit_size
        pnl = price - avg_entry_price - cost
        cash += unit_size * unit + unit * pnl * 0.01
        cash_flow.append({'cash':cash, 'pnl':pnl, 'position': abs(positions) * 1000})
        avg_entry_price = sum(entry_prices) / abs(positions)
        trade_records.append({'time': i, 'spread': price, 'avg_spread': avg_entry_price, 'direction': 'close long'})
        # pnl.append(positions * (price - avg_entry_price))  # 计算做多交易的P&L
        # cash += abs(positions) * (avg_entry_price - price)  # 更新账户现金
    
    else: 
        cash_flow.append({'cash':cash, 'pnl':pnl, 'position': abs(positions) * 1000})

trade_records_df = pd.DataFrame(trade_records)
cash_flow_df = pd.DataFrame(cash_flow)

print("交易记录表格：")
print(trade_records_df)
print(cash_flow_df)


# 绘制交易记录图表
plt.figure(figsize=(10, 6))
plt.plot(data.index, data['spread'], label='Spread')
plt.scatter(trade_records_df['time'], trade_records_df['spread'], c='red', label='Trade')
plt.axhline(y=entry_threshold, color='green', linestyle='--', label='Entry Threshold')
plt.axhline(y=-entry_threshold, color='green', linestyle='--')
plt.axhline(y=exit_threshold, color='orange', linestyle='--', label='Exit Threshold')
plt.axhline(y=-exit_threshold, color='orange', linestyle='--')
plt.xlabel('时间')
plt.ylabel('Spread')
plt.legend()
plt.title('交易记录')
plt.grid(True)
plt.show()

# %%
