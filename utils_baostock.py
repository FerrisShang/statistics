import baostock as bs
from time import sleep


class Stock:
    retried_num = 0
    RETRY_MAX_NUM = 5
    RETRY_DELAY_S = 30
    subscribe_rs = None

    @staticmethod
    def login():
        Stock.lg = bs.login()
        while Stock.retried_num < Stock.RETRY_MAX_NUM and Stock.lg.error_code != '0':
            sleep(Stock.RETRY_DELAY_S)
            Stock.lg = bs.login()
            Stock.retried_num += 1
        assert('0' == Stock.lg.error_code)
        Stock.retried_num = 0

    @staticmethod
    def logout():
        bs.logout()

    @staticmethod
    def subscribe_real_time(sub_code, sub_cb, param=None):
        pass

    @staticmethod
    def unsubscribe_real_time():
        pass

    @staticmethod
    def rs_to_list(result_data):
        data_list = []
        while (result_data.error_code == '0') & result_data.next():
            data_list.append(result_data.get_row_data())
        return data_list

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
    def query_history_k_data(code, fields, start_date=None, end_date=None, frequency='d', adjust_flag='3'):
        rs = bs.query_history_k_data(code, fields, start_date, end_date, frequency, adjust_flag)
        while Stock.retried_num < Stock.RETRY_MAX_NUM and rs.error_code != '0':
            sleep(Stock.RETRY_DELAY_S)
            bs.query_history_k_data(code, fields, start_date, end_date, frequency, adjust_flag)
            Stock.retried_num += 1
        assert('0' == rs.error_code)
        Stock.retried_num = 0
        return Stock.rs_to_list(rs)

    @staticmethod
    def query_hist_kd(code, start_date=None, end_date=None):
        fields = 'date, open, close, high, low, volume, amount, adjustflag, turn, tradestatus, '
        fields += 'pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST'
        return Stock.query_history_k_data(code, fields, start_date, end_date, frequency='d')

    @staticmethod
    def query_hist_k5(code, start_date=None, end_date=None):
        fields = 'time, open, close, high, low, volume, amount'
        return Stock.query_history_k_data(code, fields, start_date, end_date, frequency='5')

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
    Stock.login()
    for x in Stock.query_hist_kd('sh.600000', '2018-10-01'):
        print(x)
    for x in Stock.query_stock_industry():
        print(x)
    Stock.logout()
