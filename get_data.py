import pandas as pd
import sqlite3
import os
import requests
from copy import deepcopy
import datetime
from dateutil.relativedelta import relativedelta
import time
from bs4 import BeautifulSoup
import numpy as np
from database import database


class get_data:
    def __init__(self, root):
        self.root = root

    def get_stock_list(self):
        market = ['stockMkt', 'kosadqMkt']

        url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType='

        dfs = []
        for m in market:
            df = pd.read_html(url + m, header=0)[0]
            if m == 'stockMkt':
                df['market'] = '코스피'
            else:
                df['market'] = '코스닥'
            df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
            dfs.append(df)
        return pd.concat(dfs)

    def to_datetime(self, df):
        td = pd.to_timedelta(df.loc[1:, 'Date'], unit='m')
        df['Date'] = datetime.datetime(2000, 1, 1, 0, 0, 0) + relativedelta(minutes=int(df.loc[0, 'Date']))
        df.loc[1:, 'Date'] = df.loc[1:, 'Date'] + td
        return df

    def get_day_candle(self):
        url = 'https://finance-services.msn.com/Market.svc/ChartDataV5?'
        header = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9,ko;q=0.8,vi;q=0.7',
            'origin': 'https://www.msn.com',
            'referer': 'https://www.msn.com/',
            'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
            'sec-ch-ua-mobile': '?0',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'
        }
        param = {
            'symbols': '141.1.A{code}.KRX.{code}',
            'chartType': '1y',
            'isEOD': 'False',
            'lang': 'ko-KR',
            'isCS': 'true',
            'isVol': 'true'
        }

        # 데이터베이스 접속
        fpath = os.path.join(self.root, 'database', 'day_candle.db')
        con = sqlite3.connect(fpath)

        df_list = pd.read_excel("stockList.xlsx")
        df_list['종목코드'] = df_list['종목코드'].astype(str).str.zfill(6)
        for i, code in enumerate(df_list['종목코드']):
            if (i + 1) % 100 == 0 or (i + 1) == df_list.shape[0]:
                print('{} / {}'.format(i+1, df_list.shape[0]))
            param_c = deepcopy(param)
            param_c['symbols'] = param_c['symbols'].format(code=code)
            try:
                response = requests.get(url, params=param_c, headers=header)
                quote = response.json()
            except Exception as e:
                print(e)
                time.sleep(1)
                continue

            if not quote:
                continue

            if len(quote[0]['Series']) == 0:
                continue

            df = pd.DataFrame(quote[0]['Series'])
            df = df.rename(columns={'Hp': 'High', 'Lp': 'Low', 'Op': 'Open', 'P': 'Close', 'V': 'Volume', 'T': 'Date'})
            df = self.to_datetime(df)
            df['Date'] = df['Date'].dt.date
            df = df.set_index('Date')
            df[['Open', 'High', 'Low', 'Close', 'Volume']].to_sql(code, con, if_exists='replace')
            time.sleep(0.25)

    def get_minute_candle(self):
        url = 'https://finance-services.msn.com/Market.svc/ChartDataV5?'
        header = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9,ko;q=0.8,vi;q=0.7',
            'origin': 'https://www.msn.com',
            'referer': 'https://www.msn.com/',
            'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
            'sec-ch-ua-mobile': '?0',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'
        }
        param = {
            'symbols': '141.1.A{code}.KRX.{code}',
            'chartType': '1d',
            'isEOD': 'False',
            'lang': 'ko-KR',
            'isCS': 'true',
            'isVol': 'true'
        }

        # 데이터베이스 접속
        fpath = os.path.join(self.root, 'database', 'minute_candle.db')
        con = sqlite3.connect(fpath)

        df_list = pd.read_excel("stockList.xlsx")
        df_list['종목코드'] = df_list['종목코드'].astype(str).str.zfill(6)
        for i, code in enumerate(df_list['종목코드']):
            if (i + 1) % 100 == 0 or (i + 1) == df_list.shape[0]:
                print('{} / {}'.format(i+1, df_list.shape[0]))
            param_c = deepcopy(param)
            param_c['symbols'] = param_c['symbols'].format(code=code)

            try:
                response = requests.get(url, params=param_c, headers=header)
                quote = response.json()
            except Exception as e:
                print(e)
                time.sleep(1)
                continue

            if not quote:
                continue

            if len(quote[0]['Series']) == 0:
                continue

            df = pd.DataFrame(quote[0]['Series'])
            df = df.rename(columns={'Hp': 'High', 'Lp': 'Low', 'Op': 'Open', 'P': 'Close', 'V': 'Volume', 'T': 'Date'})
            df = self.to_datetime(df)
            df = df.set_index('Date')
            df[['Open', 'High', 'Low', 'Close', 'Volume']].to_sql(code, con, if_exists='replace')
            time.sleep(0.25)

    def get_fundamental(self):
        url = 'https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?'
        headers = {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Cookie': 'setC1010001=%5B%7B%22conGubun%22%3A%22MAIN%22%2C%22cTB23%22%3A%22cns_td1%22%2C%22bandChartGubun%22%3A%22MAIN%22%2C%22finGubun%22%3A%22MAIN%22%2C%22cTB00%22%3A%22cns_td20%22%7D%5D; setC1030001=%5B%7B%22cTB301%22%3A%22rpt_td1%22%2C%22finGubun%22%3A%22MAIN%22%2C%22frqTyp%22%3A%220%22%2C%22moreY%22%3A1%2C%22moreQ%22%3A1%2C%22moreQQ%22%3A1%7D%5D',
            'Host': 'navercomp.wisereport.co.kr',
            'Referer': 'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd=005930&cn=',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        params = {
            'cmp_cd': '',
            'fin_typ': '0',
            'freq_typ': 'Y',
            'encparam': 'YUhCZ09NY0FsYnozdUJCMlFTa2o2UT09',
            'id': 'RVArcVR1a2'
        }

        df_list = pd.read_excel('stockList.xlsx')
        df_list['종목코드'] = df_list['종목코드'].astype(str).str.zfill(6)
        for code in df_list['종목코드']:
            params_c = deepcopy(params)
            params_c['cmp_cd'] = code

            try:
                response = requests.get(url, params=params_c, headers=headers, timeout=(3, 3))
            except Exception as e:
                print(e)
                time.sleep(1)
                continue

            soup = BeautifulSoup(response.text)
            for i, table in enumerate(soup.findAll("table")):
                if i == 0:
                    continue

                index = []
                columns = []
                contents = []
                for j, tr in enumerate(table.findAll("tr")):
                    row = []
                    if j == 0:
                        continue
                    elif j == 1:
                        for th in tr.findAll("th"):
                            columns.append(th.text.replace("\n", "").replace("\r", "").replace("\t", "").replace("(IFRS연결)", ""))
                    else:
                        for td in tr.findAll("td"):
                            row.append(td.text.replace("\n", "").replace("\r", "").replace("\t", "").replace("(IFRS연결)", ""))
                        for th in tr.findAll("th"):
                            index.append(th.text.replace("\n", "").replace("\r", "").replace("\t", "").replace("(IFRS연결)", ""))
                    if row:
                        contents.append(row)
                df = pd.DataFrame(contents, index=index, columns=columns)
                df.to_excel(os.path.join(self.root, 'fundamental', code + '.xlsx'))

    def group_for_funda(self):
        dirname = os.path.join(self.root, 'fundamental')
        flist = os.listdir(dirname)

        fpaths = {}
        for fname in flist:
            if fname.endswith('.xlsx'):
                code = os.path.splitext(fname)[0]
                fpaths[code] = os.path.join(dirname, fname)

        dfs = {}
        period = set()
        indicator = set()
        con_out = sqlite3.connect(os.path.join(self.root, 'database', 'fundamental_y.db'))
        for code, fpath in fpaths.items():
            df = pd.read_excel(fpath, index_col=0)

            columns = []
            drop_col = []
            for col in df.columns:
                if '(E)' in col:
                    drop_col.append(col)
                    continue
                if '(IFRS별도)' in col:
                    col = col.replace('(IFRS별도)', '')
                if '(GAAP별도)' in col:
                    col = col.replace('(GAAP별도)', '')
                columns.append(col)

            df = df.drop(drop_col, axis=1)
            df.columns = columns
            dfs[code] = df
            period = set.union(set(df.columns), period)
            indicator = set.union(set(df.index), indicator)

        codeList = sorted(list(fpaths.keys()))
        for p in period:
            df_p = pd.DataFrame(index=codeList, columns=indicator)
            for code, df in dfs.items():
                if p in df.columns:
                    df_p.loc[code, df.index] = df[p]

            df_p = df_p.dropna(how='all', axis=1)
            df_p = df_p.dropna(how='all', axis=0)
            if df_p.empty:
                continue

            for col in df_p.columns:
                df_p[col] = df_p[col].str.replace(',', '').astype(float)
            df_p.index.name = '코드'
            df_p.to_sql(p.replace('/', '-'), con_out, if_exists='replace')

    def get_current_price(self, code):
        url = 'https://finance.daum.net/api/quotes/A{}'.format(code)
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'cookie': 'TIARA=j5uHnREkywj-RBM8V454HEIrVX7ELKa82aPyIyOpx7zDaEgpxG85Gjyigt8PalvdWCX.hMIrk.WIgfH1Q2EepSZgKyj8WSxJkyrYsLEYRNw0; _ga=GA1.2.80326406.1615204312; _gid=GA1.2.1461602055.1615204312; _gat_gtag_UA_128578811_1=1; KAKAO_STOCK_RECENT=[%22A005930%22]; webid=29bb49255b334e1685e80f434bbfa9f6; webid_sync=1615204317280; webid_ts=1615204311801; _gat_gtag_UA_74989022_11=1; _T_ANO=hUR5GiyqC83GwEXaCH6RoDxCMxhD1ntSNgihF4rQH6lnl4mtbujPM26+t0y0plfgCAPtKPmsLovJu9Y4DHEwfVd7dy7sm6W9/bVXsTPOixj5Re/CvNEn6ugWEWP2KvFWrzKZFhN13IjeFglmchOHJvsKf1EwWov+M5FkhstqYnNeZLRZWuefFTiH50LevUdexXeY0cbs+4rjq2xkalD7+DEzjGLkurohjrmsSgD8wJsRcU/agJ5JgZwyRRe/vb7Q2Vbolx9FmuUNz2Vaxq48gtlGA2Gvjyu2GXPUo7JPE7e2qEOKHwYcwfLQTeXNIyWCtQfjHmJzd3MvBwPqJH6DaA==; _dfs=UGNWRWtTWmx3MzhQTGViY0JIR21ub1RaQUFDYXlnckhDemVSY3pkV1o1cmdUQWN1OVpOcDZHaG0rVWd5bWswOUEyRXNrU3VjTjhLVTFMQmQ5aEpKYnc9PS0tTTRHOEJKQ0MwRjh1SkR4OWZuUms5UT09--4a47afdc899cd84aa967c9fc8cf67ee771bb2d8c'.format(
                code),
            'referer': 'https://finance.daum.net/quotes/A{}'.format(code),
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        params = {
            'summary': 'false',
            'changeStatistics': 'true'
        }

        response = requests.get(url, params=params, headers=headers)
        quote = pd.Series(response.json())
        return quote['tradePrice']


if __name__ == '__main__':
    gd = get_data("D:/stock")
    # df_list = gd.get_stock_list()
    # df_list.to_excel("stockList.xlsx", index=False)
    # gd.get_day_candle()
    # gd.get_minute_candle()
    # gd.get_fundamental()
    gd.get_current_price("005930")
