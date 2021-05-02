from pytrader import MyWindow
import pandas as pd
import datetime
from PyQt5.QtWidgets import QApplication
import sys
from threading import Thread
from PyQt5.QtCore import pyqtSignal


class Buy(Thread, MyWindow):
    tx_signal = pyqtSignal(str)
    rx_signal = pyqtSignal(str)

    def __init__(self, user_param, cond):
        Thread.__init__(self)
        MyWindow.__init__(self, user_param, cond)

        self.user_param = user_param
        self.rx_signal.connect(self.exec_order)

    def exec_order(self, command):
        if ',' in command:
            args = command.split(',')
            eval('self.' + args[0])(*args[1:])
        else:
            eval('self.' + command)()

    def finish(self):
        command = ';'.join([self.__class__.__name__, 'Sell', 'done'])
        self.tx_signal.emit(command)

    def swing(self):
        print("swing")

        print('swing ends...')
        self.finish()

    def daytrade(self):
        print("daytrade")

        print('daytrade ends...')
        self.finish()

    def ma5_kiss(self):
        label = "5일키스기법"
        print("ma5_kiss_starts...")

        now = datetime.datetime.now()
        today_close_time = str(now.date()) + ' ' + self.user_param['market-time']["close"]
        close_time = datetime.datetime.strptime(today_close_time, "%Y-%m-%d %H:%M:%S")
        if now > close_time:
            print("market closed")
            return
        
        # 조건검색식 요청
        codeList = self.get_condition(self.conditionDictionary[label])
        if len(codeList) == 0:
            print("[screener] no stock to buy")

        # 데이터 다운
        data = self.get_day_candle(codeList)
        balance = self.kiwoom.getBalance()
        if balance <= 0:
            print("not enough money to buy")
            return

        buy_list = []
        # 데이터 분석
        each_bet = balance / len(codeList)
        for code in codeList:
            if code not in data:
                continue

            df_day = data[code]
            close_price = df_day.loc['Close'].iloc[-1]
            last_close = df_day.loc['Close'].iloc[-2]
            df_day['ma5'] = df_day['Close'].rolling(5).mean()
            if df_day['ma5'].iloc[-1] < close_price and last_close < df_day['ma5'].iloc[-2]:
                buy_price = close_price
                sell_price = int(buy_price * 1.5)
                stop_price = int(buy_price * 0.9)
                due_date = (now + datetime.timedelta(weeks=2)).replace(hour=15, minute=0, second=0, microsecond=0)
                qty = int(each_bet / buy_price)
                if qty > 0:
                    buy_list.append([code, buy_price, qty, 0, sell_price, stop_price, label, str(due_date)])

        df_buy = pd.DataFrame(buy_list, columns=['code', 'buy_price', 'qty', 'cum_qty', 'sell_price', 'stop_price',
                                                 'algorithm', 'due_date'])
        # 매수주문
        self.buy_order(df_buy)

        print('mat5_kiss ends...')
        self.finish()

    def run(self):
        print("trading starts")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    buy = Buy()
    sys.exit(app.exec_())
