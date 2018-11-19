import baostock as bs
import pandas as pd
import numpy as np
from time import sleep


class BaoStock:
    retried_num = 0
    RETRY_MAX_NUM = 5
    RETRY_DELAY_S = 30
    subscribe_rs = None

    @staticmethod
    def login():
        BaoStock.lg = bs.login()
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and BaoStock.lg.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            BaoStock.lg = bs.login()
            BaoStock.retried_num += 1
        assert('0' == BaoStock.lg.error_code)
        BaoStock.retried_num = 0

    @staticmethod
    def logout():
        bs.logout()

    @staticmethod
    def subscribe_real_time(sub_code, sub_cb, param=None):
        rs = bs.login_real_time()
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            rs = bs.login_real_time()
            BaoStock.retried_num += 1
        assert('0' == rs.error_code)
        BaoStock.retried_num = 0
        BaoStock.subscribe_rs = bs.subscribe_by_code(sub_code, 0, sub_cb, param)
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and BaoStock.subscribe_rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            BaoStock.subscribe_rs = bs.subscribe_by_code(sub_code, 0, sub_cb, param)
            BaoStock.retried_num += 1
        assert('0' == BaoStock.subscribe_rs.error_code)
        BaoStock.retried_num = 0

    @staticmethod
    def unsubscribe_real_time():
        if BaoStock.subscribe_rs is not None:
            bs.cancel_subscribe(BaoStock.subscribe_rs.serial_id)
            BaoStock.subscribe_rs = None
        bs.logout_real_time()

    @staticmethod
    def rs_to_list(result_data):
        data_list = []
        while (result_data.error_code == '0') & result_data.next():
            data_list.append(result_data.get_row_data())
        result = pd.DataFrame(data_list, columns=result_data.fields)
        result_array = np.array(result)
        return result_array.tolist()

    @staticmethod
    def query_basic(code='', code_name=''):
        assert(isinstance(code, str))
        rs = bs.query_stock_basic(code, code_name)
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            rs = bs.query_stock_basic(code, code_name)
            BaoStock.retried_num += 1
        assert('0' == rs.error_code)
        BaoStock.retried_num = 0
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_history_k_data(code, fields, start_date=None, end_date=None, frequency='d', adjust_flag='3'):
        rs = bs.query_history_k_data(code, fields, start_date, end_date, frequency, adjust_flag)
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            bs.query_history_k_data(code, fields, start_date, end_date, frequency, adjust_flag)
            BaoStock.retried_num += 1
        assert('0' == rs.error_code)
        BaoStock.retried_num = 0
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_hist_kd(code, start_date=None, end_date=None):
        fields = 'date, open, close, high, low, volume, amount, adjustflag, turn, tradestatus, '
        fields += 'pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST'
        return BaoStock.query_history_k_data(code, fields, start_date, end_date, frequency='d')

    @staticmethod
    def query_hist_k5(code, start_date=None, end_date=None):
        fields = 'time, open, close, high, low, volume, amount'
        return BaoStock.query_history_k_data(code, fields, start_date, end_date, frequency='5')

    @staticmethod
    def query_stock_industry(code='', date=''):
        rs = bs.query_stock_industry(code, date)
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            rs = bs.query_stock_industry(code, date)
            BaoStock.retried_num += 1
        assert('0' == rs.error_code)
        BaoStock.retried_num = 0
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_hs300_stocks():
        rs = bs.query_hs300_stocks()
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            rs = bs.query_hs300_stocks()
            BaoStock.retried_num += 1
        assert('0' == rs.error_code)
        BaoStock.retried_num = 0
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_sz50_stocks():
        rs = bs.query_sz50_stocks()
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            rs = bs.query_sz50_stocks()
            BaoStock.retried_num += 1
        assert('0' == rs.error_code)
        BaoStock.retried_num = 0
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_zz500_stocks():
        rs = bs.query_zz500_stocks()
        while BaoStock.retried_num < BaoStock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(BaoStock.RETRY_DELAY_S)
            rs = bs.query_zz500_stocks()
            BaoStock.retried_num += 1
        assert('0' == rs.error_code)
        BaoStock.retried_num = 0
        return BaoStock.rs_to_list(rs)

if __name__ == '__main__':
    BaoStock.login()
    for x in BaoStock.query_hist_kd('sh.600000', '2018-10-01'):
        print(x)
    for x in BaoStock.query_stock_industry():
        print(x)
    BaoStock.logout()
