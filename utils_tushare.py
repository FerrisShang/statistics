import tushare as ts
import baostock as bs
from time import sleep


class Stock:
    retried_num = 0
    RETRY_MAX_NUM = 5
    RETRY_DELAY_S = 30
    subscribe_rs = None

    @staticmethod
    def login():
        pass

    @staticmethod
    def logout():
        pass

    @staticmethod
    def subscribe_real_time(sub_code, sub_cb, param=None):
        pass

    @staticmethod
    def unsubscribe_real_time():
        pass

    @staticmethod
    def rs_to_list(result_data):
        return result_data

    @staticmethod
    def query_basic(code='', code_name=''):
        assert(isinstance(code, str))
        rs = bs.query_stock_basic(code, code_name)
        while Stock.retried_num < Stock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(Stock.RETRY_DELAY_S)
            rs = bs.query_stock_basic(code, code_name)
            Stock.retried_num += 1
        assert('0' == rs.error_code)
        Stock.retried_num = 0
        return Stock.rs_to_list(rs)

    @staticmethod
    def query_hist_kd(code, start_date=None, end_date=None):
        fields = 'date, open, close, high, low, volume, amount, adjustflag, turn, tradestatus, '
        fields += 'pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST'

        code = code[-6:]
        rs = ts.get_k_data(code, ktype='d', start=start_date, end=end_date)
        data = rs[['date', 'open', 'close', 'high', 'low', 'volume']].values.tolist()
        res = []
        for i in range(len(data)):
            res.append(data[i][:-1] + [int(data[i][-1])*100] + [0] * 10)
        return res

    @staticmethod
    def query_hist_k5(code, start_date='', end_date='2099-12-31'):
        fields = 'time, open, close, high, low, volume, amount'  # WARNING: amount NOT used any more
        code = code[-6:]
        rs = ts.get_hist_data(code, ktype='5', start=start_date, end=end_date)
        data = rs[['open', 'close', 'high', 'low', 'volume']].values.tolist()
        date = rs.index.tolist()
        res = []
        for i in range(len(data)):
            res.append([int(date[i][2:4]+date[i][5:7]+date[i][8:10]+date[i][11:13]+date[i][14:16])] + data[i][:-1] + [int(data[i][-1])*100] + [0])
        return res

    @staticmethod
    def query_stock_industry(code='', date=''):
        rs = bs.query_stock_industry(code, date)
        while Stock.retried_num < Stock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(Stock.RETRY_DELAY_S)
            rs = bs.query_stock_industry(code, date)
            Stock.retried_num += 1
        assert('0' == rs.error_code)
        Stock.retried_num = 0
        return Stock.rs_to_list(rs)

    @staticmethod
    def query_hs300_stocks():
        rs = bs.query_hs300_stocks()
        while Stock.retried_num < Stock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(Stock.RETRY_DELAY_S)
            rs = bs.query_hs300_stocks()
            Stock.retried_num += 1
        assert('0' == rs.error_code)
        Stock.retried_num = 0
        return Stock.rs_to_list(rs)

    @staticmethod
    def query_sz50_stocks():
        rs = bs.query_sz50_stocks()
        while Stock.retried_num < Stock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(Stock.RETRY_DELAY_S)
            rs = bs.query_sz50_stocks()
            Stock.retried_num += 1
        assert('0' == rs.error_code)
        Stock.retried_num = 0
        return Stock.rs_to_list(rs)

    @staticmethod
    def query_zz500_stocks():
        rs = bs.query_zz500_stocks()
        while Stock.retried_num < Stock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(Stock.RETRY_DELAY_S)
            rs = bs.query_zz500_stocks()
            Stock.retried_num += 1
        assert('0' == rs.error_code)
        Stock.retried_num = 0
        return Stock.rs_to_list(rs)

if __name__ == '__main__':
    # for x in Stock.query_hist_k5('002602', '2019-04-15', '2099-12-31'):
    #     print(x)
    # for x in Stock.query_hist_kd('002602', '2017-03-16', '2099-12-31'):
    #     print(x)
    from utils_data import *
    s = StockUpdateRecord('sz.002602')
    s.update_k5()
    s.update_kd()
