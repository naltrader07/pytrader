from PyQt5.QtWidgets import QApplication
import sys
from threading import Condition
from sell import Sell
from buy import Buy
import json


class Main:
    def __init__(self):
        self.threads = {}

    def load_param(self):
        with open("parameter.json") as json_file:
            return json.load(json_file)

    def connect_order(self, command):
        from_signal, to_signal, func = command.split(';')
        self.threads[to_signal].rx_signal.emit(func)

    def run(self):
        cond = Condition()
        user_param = self.load_param()
        app = QApplication(sys.argv)
        for thread in [Buy, Sell]:
            cname = thread.__name__
            self.threads[cname] = thread(user_param, cond)
            self.threads[cname].tx_signal.connect(self.connect_order)
            self.threads[cname].start()
        sys.exit(app.exec_())


if __name__ == '__main__':
    main = Main()
    main.run()
