import sqlite3
import pandas as pd
import os
from database import database


class sim_ma5:
    def __init__(self, root):
        self.root = root

    def run(self):
        con = sqlite3.connect(os.path.join(self.root, 'database', 'day_candle.db'))
        db = database()
        codeList = db.load_tableList(con)

        result = []
        for code in codeList:
            df = pd.read_sql("SELECT * FROM '{}'".format(code), con, index_col='Date')
            df['ma5'] = df['Close'].rolling(5).mean()
            buy_dates = df[(df['Open'] < df['ma5']) & (df['Close'] > df['ma5'])].index
            sell_dates = df[(df['Open'] > df['Close']) & (df['Close'] < df['ma5'])].index

            df_buy = pd.Series(data=['buy']*len(buy_dates), index=buy_dates)
            df_sell = pd.Series(data=['sell']*len(sell_dates), index=sell_dates)
            df_dates = pd.concat([df_buy, df_sell]).sort_index()

            side = 'buy'
            for date in df_dates.index:
                if df_dates[date] != side:
                    continue

                if side == 'buy':
                    buy_price = df.loc[date, 'Close']
                    result.append([code, date, buy_price, None, None])
                    side = 'sell'
                else:
                    sell_price = df.loc[date, 'Close']
                    result[-1][-2] = date
                    result[-1][-1] = sell_price
                    side = 'buy'
        df_res = pd.DataFrame(result, columns=['code', 'date', 'buy_price', 'sell_date', 'sell_price'])
        df_res['profit'] = (df_res['sell_price'] - df_res['buy_price']) / df_res['buy_price']
        df_res.to_excel(os.path.join(self.root, 'simulation', '5일선기법.xlsx'), index=False)


if __name__ == '__main__':
    s5 = sim_ma5("D:/stock")
    s5.run()