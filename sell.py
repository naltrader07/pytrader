from pytrader import MyWindow
import pandas as pd
import datetime
import json
from get_data import get_data
import sqlite3
import os
import time
from PyQt5.QtCore import QThread, pyqtSignal, QEventLoop
import pause
from order import Order
from dateutil.relativedelta import relativedelta


class Sell(QThread):
    tx_signal = pyqtSignal(str)
    rx_signal = pyqtSignal(str)

    def __init__(self, user_param, cond):
        super(Sell, self).__init__()
        self.user_param = user_param
        self.cond = cond
        self.rx_signal.connect(self.exec_order)

    def exec_order(self, command):
        if ',' in command:
            args = command.split(',')
            eval('self.' + args[0])(*args[1:])
        else:
            eval('self.' + command)()

    def send(self, command):
        # command를 buy에 보낼때
        self.tx_signal.emit(command)
        self.orderLoop = QEventLoop()
        self.orderLoop.exec_()

    def done(self):
        # buy에서 task를 마무리할때
        if self.orderLoop is not None:
            self.orderLoop.exit()

    def __set_time_format(self, date, time):
        return datetime.datetime.strptime(date + ' ' + time, "%Y-%m-%d %H:%M:%S")

    def monitor(self, close_time):
        gd = get_data(self.user_param['path']['root'])
        con = sqlite3.connect(os.path.join(self.user_param['path']['root'], 'database', 'order.db'))
        df = pd.read_sql("SELECT * FROM 'hold_list'", con, index_col='tid')

        today = str(datetime.date.today())
        open_time = datetime.datetime.strptime(today + ' ' + self.user_param['market-time']['open'], "%Y-%m-%d %H:%M:%S")

        while open_time <= datetime.datetime.now() < close_time:
            for tid in df.index:
                code = df.loc[tid, 'code']
                sell_price = df.loc[tid, 'sell_price']
                stop_price = df.loc[tid, 'stop_price']
                due_date = df.loc[tid, 'due_date']
                now = datetime.datetime.now()

                trade_price = gd.get_current_price(code)
                # 익절
                if (sell_price - trade_price) / trade_price < 0.01:
                    command = ';'.join([self.__class__.__name__, 'Buy', 'sell_order,{},지정가'.format(tid)])
                    self.send(command)

                # 손절
                if trade_price <= stop_price:
                    command = ';'.join([self.__class__.__name__, 'Buy', 'sell_order,{},시장가'.format(tid)])
                    self.send(command)

                # 타임오버
                if due_date <= now:
                    command = ';'.join([self.__class__.__name__, 'Buy', 'sell_order,{},시장가'.format(tid)])
                    self.send(command)

                time.sleep(0.5)
            time.sleep(10)

    def run(self):
        today = str(datetime.date.today())

        open_time = self.__set_time_format(today, self.user_param['market-time']['open'])
        close_time = self.__set_time_format(today, self.user_param['market-time']['close'])

        dt_time = self.__set_time_format(today, self.user_param['market-time']['daytrade'])
        swing_time = self.__set_time_format(today, self.user_param['market-time']['swing'])

        now = datetime.datetime.now()
        interval = 60
        if open_time <= now < close_time:
            dt_time = Order.roundTime(now, interval)
            print("monitor until {}".format(dt_time))
            self.monitor(dt_time)

            # day trading
            while datetime.datetime.now() < close_time - relativedelta(minutes=30):
                command = ';'.join([self.__class__.__name__, 'Buy', 'daytrade'])
                self.send(command)

                dt_time += relativedelta(seconds=interval)
                print("monitor until {}".format(dt_time))
                self.monitor(dt_time)

            print("monitor until {}".format(swing_time))
            self.monitor(swing_time)

            command = ';'.join([self.__class__.__name__, 'Buy', 'swing'])
            self.send(command)
        else:
            print("market is closed")
