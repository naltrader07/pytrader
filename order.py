import sqlite3
import os
import pandas as pd
import datetime
import time
from write_db import Write_db


class Order(Write_db):
    def __init__(self):
        self.columns = ['tid', 'symbol', 'name', 'id', 'qty', 'cum_qty', 'buy_price', 'buy_datetime', 'sell_price',
                        'sell_datetime', 'stop_price', 'due_date', 'profit', 'trade', 'state', 'algorithm']
        self.hogaTypeTable = {"지정가": "00", "시장가": "03"}

    @staticmethod
    def roundTime(dt=None, roundTo=60, round='up'):
        """Round a datetime object to any time lapse in seconds
        dt : datetime.datetime object, default now.
        roundTo : Closest number of seconds to round to, default 1 minute.
        Author: Thierry Husson 2012 - Use it as you want but don't blame me.
        """
        if dt is None:
            dt = datetime.datetime.now()
        if round == 'up':
            N = 1
        else:
            N = 2
        seconds = (dt.replace(tzinfo=None) - dt.min).seconds
        rounding = (seconds + roundTo / N) // roundTo * roundTo
        return dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)

    def set_hoga_price(self, df):
        def get_hoga(buy_price, n):
            return int(((buy_price + n - 1) // n) * n)

        df_list = pd.read_excel("stockList.xlsx")
        df_list['종목코드'] = df_list['종목코드'].astype(str).zfill(6)
        df_list.set_index('종목코드', drop=True, inplace=True)

        for idx in df.index:
            for side in ['buy_price', 'sell_price', 'stop_price']:
                buy_price = int(df.loc[idx, side])
                if buy_price < 1000:
                    continue

                code = df.loc[idx, 'code']
                if code not in df_list.index:
                    continue

                market = df.loc[code, "종목"]
                if market == "코스피":
                    if buy_price < 5000:
                        df.loc[idx, side] = get_hoga(buy_price, 5)
                    elif buy_price < 10000:
                        df.loc[idx, side] = get_hoga(buy_price, 10)
                    elif buy_price < 50000:
                        df.loc[idx, side] = get_hoga(buy_price, 50)
                    elif buy_price < 100000:
                        df.loc[idx, side] = get_hoga(buy_price, 100)
                    elif buy_price < 500000:
                        df.loc[idx, side] = get_hoga(buy_price, 500)
                    else:
                        df.loc[idx, side] = get_hoga(buy_price, 1000)
                else:
                    if buy_price < 5000:
                        df.loc[idx, side] = get_hoga(buy_price, 5)
                    elif buy_price < 10000:
                        df.loc[idx, side] = get_hoga(buy_price, 10)
                    else:
                        df.loc[idx, side] = get_hoga(buy_price, 50)
        return df

    def buy_order(self, df_buy_list):
        print("buy_order starts...")
        con = sqlite3.connect(os.path.join(self.__dict__['user_param']['path']['root'], 'database', 'order.db'))

        df_buy_list = self.set_hoga_price(df_buy_list)
        for idx in df_buy_list.index:
            code = df_buy_list.loc[idx, 'code']
            qty = df_buy_list.loc[idx, 'qty']
            buy_price = df_buy_list.loc[idx, 'buy_price']
            sell_price = df_buy_list.loc[idx, 'sell_price']
            stop_price = df_buy_list.loc[idx, 'stop_price']
            algorithm = df_buy_list.loc[idx, 'algorithm']
            due_date = df_buy_list.loc[idx, 'due_date']

            account = self.__dict__['kiwoom'].account
            if buy_price == 0:
                hoga = self.hogaTypeTable["시장가"]
            else:
                hoga = self.hogaTypeTable["지정가"]
            self.__dict__['kiwoom'].sendOrder("자동매수주문", "0101", account, 1, code, qty, buy_price, hoga, "")
            orderNo = self.__dict__['kiwoom'].orderNo

            if orderNo:
                buy_datetime = str(datetime.datetime.now().replace(microsecond=0))
                name = self.__dict__['kiwoom'].getMasterCodeName(code)
                tid = code + "_" + algorithm
                order_list = [tid, code, name, orderNo, qty, 0, buy_price, buy_datetime, sell_price, None,
                               stop_price, due_date, 0, 'buy', 'yet', algorithm]
                self.append_db(con, "order_list", order_list)
                self.__dict__['kiwoom'].orderNo = ""
                print("[{}] order is done".format(code))
            time.sleep(0.3)
        print("buy_order ends...")

    def sell_order(self, tid, orderType="지정가"):
        print("buy_order starts...")
        con = sqlite3.connect(os.path.join(self.__dict__['user_param']['path']['root'], 'database', 'order.db'))

        df_sell = pd.read_sql("SELECT * FROM 'hold_list' WHERE tid='{}'".format(tid), con, index_col='tid')
        code = df_sell.loc[tid, 'code']
        qty = df_sell.loc[tid, 'qty']
        buy_price = df_sell.loc[tid, 'buy_price']
        sell_price = df_sell.loc[tid, 'sell_price']

        account = self.__dict__['kiwoom'].account
        hoga = self.hogaTypeTable[orderType]
        self.__dict__['kiwoom'].sendOrder("자동매도주문", "0101", account, 1, code, qty, sell_price, hoga, "")
        orderNo = self.__dict__['kiwoom'].orderNo

        if orderNo:
            sell_datetime = str(datetime.datetime.now().replace(microsecond=0))
            order_list = {"id": orderNo, "sell_price": sell_price, "sell_datetime": sell_datetime, "trade": "sell", "state": "yet"}
            self.update_db(con, "hold_list", order_list)
            self.__dict__['kiwoom'].orderNo = ""
            print("[{}] order is done".format(code))

        time.sleep(0.3)
        print("buy_order ends...")
