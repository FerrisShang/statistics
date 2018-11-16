import baostock as bs
import pandas as pd
import numpy as np


class BaoStock:
    @staticmethod
    def login():
        BaoStock.lg = bs.login()
        assert('0' == BaoStock.lg.error_code)

    @staticmethod
    def logout():
        bs.logout()

    @staticmethod
    def rs_to_list(result_data):
        data_list = []
        assert(isinstance(result_data, object))
        while (result_data.error_code == '0') & result_data.next():
            data_list.append(result_data.get_row_data())
        result = pd.DataFrame(data_list, columns=result_data.fields)
        result_array = np.array(result)
        return result_array.tolist()

    @staticmethod
    def query_basic(code='', code_name=''):
        assert(isinstance(code, str))
        rs = bs.query_stock_basic(code, code_name)
        assert('0' == rs.error_code)
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_history_k_data(code, fields, start_date=None, end_date=None, frequency='d', adjust_flag='3'):
        rs = bs.query_history_k_data(code, fields, start_date, end_date, frequency, adjust_flag)
        assert('0' == rs.error_code)
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
    def query_stock_industry(code):
        rs = bs.query_stock_industry(code)
        assert('0' == rs.error_code)
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_hs300_stocks():
        rs = bs.query_hs300_stocks()
        assert('0' == rs.error_code)
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_sz50_stocks():
        rs = bs.query_sz50_stocks()
        assert('0' == rs.error_code)
        return BaoStock.rs_to_list(rs)

    @staticmethod
    def query_zz500_stocks():
        rs = bs.query_zz500_stocks()
        assert('0' == rs.error_code)
        return BaoStock.rs_to_list(rs)

if __name__ == '__main__':
    BaoStock.login()
    for x in BaoStock.query_hist_kd('sz.300223', '2018-11-01'):
        print(x)
    BaoStock.logout()