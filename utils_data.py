# -*- coding:utf-8 -*-
import os
import datetime
import requests
import time
import csv
from collections import OrderedDict
from utils_config import UtilsConfig
from enum import IntEnum
from struct import pack, unpack
from utils_baostock import *
from copy import deepcopy
from threading import Thread
from pinyin import *
try:
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
except:
    pass

__all__ = [
    'Data5',
    'DataD',
    'DataRt',
    'StockType',
    'StockStatus',
    'StockTradeStatus',
    'StockBasicInfo',
    'StocksBasicInfo',
    'StockSuperiorInfo',
    'StocksSuperiorInfo',
    'StockIndustryInfo',
    'StocksIndustryInfo',
    'StockData',
    'StockRtData',
    'StockUpdateRecord',
    'get_weekday',
]


def get_weekday(date):
    week_str = ['一', '二', '三', '四', '五', '六', '日']
    if isinstance(date, str):
        date = datetime.datetime(int(date[0:2]), int(date[2:4]), int(date[4:6]))
    else:
        date = datetime.datetime(date//10000 % 100, date//100 % 100, date % 100)
    return date.weekday(), week_str[date.weekday()]


class StockType(IntEnum):
    STOCK = 1
    INDEX = 2
    OTHER = 3


class StockStatus(IntEnum):
    LISTING = 1
    DELISTING = 0


class StockTradeStatus(IntEnum):
    ON = 1
    OFF = 0
    DATA_ERROR = 0xFFFFFFFF


class StockBasicInfo:
    area = {'3': 'sz', '0': 'sz', '6': 'sh'}

    def __init__(self, code, code_name, ipo_date, out_date, stock_type, status):
        self.code = code
        self.key = int(self.code[3:])
        self.code_name = code_name
        self.ipoDate = int(str(ipo_date).replace('-', ''))
        if len(str(out_date).replace('-', '')) == 0:
            self.outDate = 20991231
        else:
            self.outDate = int(str(out_date).replace('-', ''))
        self.type = StockType(int(stock_type))
        self.status = StockStatus(int(status))

    def __str__(self):
        return '{} {} {} {:06d} {:06d} {}'.format(self.code, self.type.name, self.status.name,
                                                  self.ipoDate, self.outDate, self.code_name)

    @staticmethod
    def code2bao(code):
        return StockBasicInfo.area[code[-6]] + '.' + code[-6:]

    @staticmethod
    def code2sina(code):
        return StockBasicInfo.area[code[-6]] + code[-6:]


class StocksBasicInfo:
    def __init__(self):
        self.infolist = OrderedDict()

    def load_from_file(self, file_name='default.list'):
        path = UtilsConfig.get_stock_list_path(file_name)
        if path is not None and os.path.isfile(path):
            try:
                f = open(path, 'r', encoding=UtilsConfig.get_encoding())
            except:
                f = open(path, 'r')
            for line in f.readlines():
                info = StockBasicInfo(*line.split())
                self.infolist[info.key] = info
            f.close()
            return True
        else:
            print('load file {} failed.'.format(file_name))
            return False

    def load_from_server(self):
        info_list = Stock.query_basic()
        for line in info_list:
            info = StockBasicInfo(*line)
            self.infolist[info.key] = info
        return True

    def save_to_file(self, file_name='default.list'):
        path = UtilsConfig.get_stock_list_path(file_name)
        if path is not None:
            try:
                f = open(path, 'w', encoding=UtilsConfig.get_encoding())
            except:
                f = open(path, 'w')
            for key, line in self.infolist.items():
                assert(isinstance(line, StockBasicInfo))
                f.write('{} {} {} {} {} {}\n'.format(line.code, line.code_name, line.ipoDate, line.outDate,
                        line.type.numerator, line.status.numerator))
            f.close()
            return True
        else:
            print('save file {} failed.'.format(file_name))
            return False

    def add(self, code, code_name, ipo_date, out_date, stock_type, status):
        info = StockBasicInfo(code, code_name, ipo_date, out_date, stock_type, status)
        self.infolist[info.key] = info

    def add_instance(self, instance):
        assert(isinstance(instance, StockBasicInfo))
        self.infolist[instance.key] = deepcopy(instance)

    def remove(self, code):
        try:
            if isinstance(code, str):
                self.infolist.pop(int(code[3:]))
            elif isinstance(code, int):
                self.infolist.pop(code)
        except KeyError:
            pass

    def get_list(self):
        return [item for key, item in self.infolist.items()]

    def get_dict(self):
        return self.infolist

    def dump(self):
        for key, value in self.infolist.items():
            print(value)

    def get_pinyin(self, code):
        if isinstance(code, str):
            code = int(code[-6:])
        code_dict = self.get_dict()
        if code not in code_dict:
            return '????'
        return ''.join([PinYin.get(c)[0][0] for c in code_dict[code].code_name]).upper()


class StockSuperiorInfo:
    def __init__(self, update_date, code, code_name):
        self.key = int(code[3:])
        self.update_date = update_date
        self.code = code
        self.code_name = code_name

    def __str__(self):
        return '{} {} {}'.format(self.update_date, self.code, self.code_name)


class StocksSuperiorInfo:
    TYPE_HZ300 = 0
    TYPE_ZZ500 = 1
    TYPE_SZ50 = 2

    def __init__(self, stocks_type=TYPE_HZ300):
        self.type = stocks_type
        self.infolist = {}
        if stocks_type == self.TYPE_ZZ500:
            self.list_name = 'zz500.list'
            self.stock_get_func = Stock.query_zz500_stocks
        elif stocks_type == self.TYPE_SZ50:
            self.list_name = 'sz50.list'
            self.stock_get_func = Stock.query_sz50_stocks
        else:
            self.list_name = 'hz300.list'
            self.stock_get_func = Stock.query_hs300_stocks

    def load_from_file(self):
        path = UtilsConfig.get_stock_list_path(self.list_name)
        if path is not None and os.path.isfile(path):
            try:
                f = open(path, 'r', encoding=UtilsConfig.get_encoding())
            except:
                f = open(path, 'r')
            for line in f.readlines():
                info = StockSuperiorInfo(*line.split())
                self.infolist[info.key] = info
            f.close()
            return True
        else:
            print('load file {} failed.'.format(self.list_name))
            return False

    def load_from_server(self):
        info_list = self.stock_get_func()
        for line in info_list:
            info = StockSuperiorInfo(*line)
            self.infolist[info.key] = info
        return True

    def save_to_file(self):
        path = UtilsConfig.get_stock_list_path(self.list_name)
        if path is not None:
            try:
                f = open(path, 'w', encoding=UtilsConfig.get_encoding())
            except:
                f = open(path, 'w')
            for key, line in self.infolist.items():
                assert(isinstance(line, StockSuperiorInfo))
                f.write('{} {} {}\n'.format(line.update_date, line.code, line.code_name))
            f.close()
            return True
        else:
            print('save file {} failed.'.format(self.list_name))
            return False

    def get_list(self):
        return [item for key, item in self.infolist.items()]

    def get_dict(self):
        return self.infolist

    def dump(self):
        for key, value in self.infolist.items():
            print(value)


class StockIndustryInfo:
    def __init__(self, update_date, code, code_name, industry, ind_class):
        self.key = int(code[3:])
        self.update_date = update_date
        self.code = code
        self.code_name = code_name
        self.industry = industry if industry != '' else '-'
        self.ind_class = ind_class if ind_class != '' else '-'

    def __str__(self):
        return '{} {} {} {} {}'.format(self.update_date, self.code, self.code_name,
                                       self.industry, self.ind_class)


class StocksIndustryInfo:
    def __init__(self):
        self.infolist = {}
        self.list_name = 'industry.list'

    def load_from_file(self):
        path = UtilsConfig.get_stock_list_path(self.list_name)
        if path is not None and os.path.isfile(path):
            try:
                f = open(path, 'r', encoding=UtilsConfig.get_encoding())
            except:
                f = open(path, 'r')
            for line in f.readlines():
                info = StockIndustryInfo(*line.split())
                self.infolist[info.key] = info
            f.close()
            return True
        else:
            print('load file {} failed.'.format(self.list_name))
            return False

    def load_from_server(self):
        info_list = Stock.query_stock_industry()
        for line in info_list:
            info = StockIndustryInfo(*line)
            self.infolist[info.key] = info
        return True

    def save_to_file(self):
        path = UtilsConfig.get_stock_list_path(self.list_name)
        if path is not None:
            try:
                f = open(path, 'w', encoding=UtilsConfig.get_encoding())
            except:
                f = open(path, 'w')
            for key, line in self.infolist.items():
                assert(isinstance(line, StockIndustryInfo))
                f.write('{} {} {} {} {}\n'.format(line.update_date, line.code, line.code_name,
                                                  line.industry, line.ind_class))
            f.close()
            return True
        else:
            print('save file {} failed.'.format(self.list_name))
            return False

    def get_list(self):
        return [item for key, item in self.infolist.items()]

    def get_dict(self):
        return self.infolist

    def dump(self):
        for key, value in self.infolist.items():
            print(value)


class StockData:
    NEW_FILE_DATE = '150101'

    def __init__(self, code, load_kd=0, load_k5=0, sync=False, start_date=0, end_date=999999):
        assert(isinstance(code, str))
        self.code = code
        self.kd_list = []
        self.k5_list = []
        self.sync = sync
        self.sync_len = 0
        if load_kd > 0:
            self.load_kd(load_kd, start_date, end_date)
        if load_k5 > 0:
            self.load_k5(load_k5, start_date, end_date)
        if sync:
            if len(self.kd_list) <= 1 or len(self.k5_list) == 0 or \
                    (self.kd_list[-1].date_num != self.k5_list[-1][0].date_num and
                     self.kd_list[-2].date_num != self.k5_list[-1][0].date_num):
                self.sync_len = 0
            else:
                if self.kd_list[-1].date_num != self.k5_list[-1][0].date_num:
                    del(self.kd_list[-1])
                max_len = min(len(self.kd_list), len(self.k5_list))
                for i in range(-1, -max_len-1, -1):
                    if self.kd_list[i].date_num == self.k5_list[i][0].date_num:
                        self.sync_len += 1
                    else:
                        break

    @staticmethod
    def parse_hex_k5(path, days, start_date=0, end_date=999999):
        ret_list = []
        data_list = []
        size = os.path.getsize(path)
        if 0 == (size % Data5.HEX_LEN):
            f = open(path, 'rb')
            if days < size//Data5.HEX_LEN//48:
                f.seek(-days * Data5.HEX_LEN * 48, 2)  # from the end of file
            else:
                days = size//Data5.HEX_LEN//48
            all_data = f.read(days * 48 * Data5.HEX_LEN)
            for i in range(days * 48):
                item_data = all_data[i * Data5.HEX_LEN:(i+1) * Data5.HEX_LEN]
                data = Data5(*unpack(Data5.FMT_HEX, item_data))
                if start_date <= data.date_num:
                    if data.date_num <= end_date:
                        data_list.append(data)
                    else:
                        break
            f.close()
            if (len(data_list) % 48) != 0:  # lose part data of a day
                print('Data seems wrong: {}  {}'.format(path, len(data_list)))
            for i in range(len(data_list) // 48):
                ret_list.append(data_list[i * 48:(i+1) * 48])
            return ret_list
        else:
            print('File size error: ' + path)

    @staticmethod
    def parse_hex_kd(path, days, start_date=0, end_date=999999):
        ret_list = []
        size = os.path.getsize(path)
        if 0 == (size % DataD.HEX_LEN):
            f = open(path, 'rb')
            if days < size//DataD.HEX_LEN:
                f.seek(-days * DataD.HEX_LEN, 2)  # from the end of file
            else:
                days = size//DataD.HEX_LEN
            all_data = f.read(days * DataD.HEX_LEN)
            for i in range(days):
                item_data = all_data[i * DataD.HEX_LEN:(i+1)*DataD.HEX_LEN]
                params = unpack(DataD.FMT_HEX, item_data)
                data = DataD(*(params[:5]+params[6:]))  # ignore unused integer
                if start_date <= data.date_num:
                    if data.date_num <= end_date:
                        ret_list.append(data)
                    else:
                        break
            f.close()
            return ret_list
        else:
            print('File size error: ' + path)

    def load_kd(self, days, start_date=0, end_date=999999):
        path = UtilsConfig.get_stock_data_path(self.code, stock_type='kd')
        if path is not None and os.path.isfile(path):
            self.kd_list = StockData.parse_hex_kd(path, days, start_date, end_date)
            return True
        else:
            # print(self.code, 'Load kd error')
            return False

    def load_k5(self, days, start_date=0, end_date=999999):
        path = UtilsConfig.get_stock_data_path(self.code, stock_type='k5')
        if path is not None and os.path.isfile(path):
            self.k5_list = StockData.parse_hex_k5(path, days, start_date, end_date)
            return True
        else:
            # print(self.code, 'Load k5 error')
            return False


class Data5:
    FMT_HEX = '<LffffLf'
    HEX_LEN = 4*7

    def __init__(self, v_time, v_open, v_close, v_high, v_low, volume, amount):
        # ['20181109150000000', '18.5400', '18.5200', '18.5500', '18.5000', '99200', '1837097.0000']
        if isinstance(v_time, int):
            self.time_str = str(v_time)
        else:
            self.time_str = v_time[2:12]
        self.date_num = int(self.time_str[:6])
        self.open = float(v_open)
        self.close = float(v_close)
        self.high = float(v_high)
        self.low = float(v_low)
        self.volume = int(volume)
        self.amount = float(amount)

    def __str__(self):
        return '{}  {:6.2f}  {:6.2f}  {:6.2f}  {:6.2f}  {:10d}  {:.0f}'. \
            format(self.time_str, self.open, self.close,
                   self.high, self.low, self.volume, self.amount)


class DataD:
    FMT_HEX = '<LffffLQfLfLfffffL'
    HEX_LEN = 4*18

    def __init__(self, date, v_open, v_close, v_high, v_low, volume, amount, adjust_flag,
                 turn, trade_status, pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST):
        # ['2018-11-01', '18.9000', '18.2200', '19.1200', '18.1100', '4301411', '79888895.9000', '3',
        # '3.587307', '1', '1.053792', '179.076255', '16.478888', '71.301441', '3.187132', '0']
        if isinstance(date, int):
            self.date_str = str(date)
        else:
            self.date_str = date[2:4]+date[5:7]+date[8:10]
        try:
            self.date_num = int(self.date_str)
            self.open = float(v_open)
            self.close = float(v_close)
            self.high = float(v_high)
            self.low = float(v_low)
            self.volume = int(volume)
            self.amount = float(amount)
            self.adjust_flag = int(adjust_flag)
            self.turn = float(0 if turn == '' else turn)
            self.trade_status = StockTradeStatus(int(trade_status))
            self.pctChg = float(0 if pctChg == '' else pctChg)
            self.peTTM = float(peTTM)
            self.psTTM = float(psTTM)
            self.pcfNcfTTM = float(pcfNcfTTM)
            self.pbMRQ = float(pbMRQ)
            self.isST = bool(isST)
        except ValueError:
            self.open, self.close, self.high, self.low = 0.0, 0.0, 0.0, 0.0
            self.volume, self.amount, self.adjust_flag, self.turn = 0, 0.0, 0, 0.0
            self.trade_status = StockTradeStatus.DATA_ERROR
            self.pctChg, self.peTTM, self.psTTM, self.pcfNcfTTM = 0.0, 0.0, 0.0, 0.0
            self.pbMRQ, self.isST = 0.0, False

    def __str__(self):
        return '{}({})  {:6.2f}  {:6.2f}  {:6.2f}  {:6.2f}  {:10d}  {:5.2f}  {:.0f}  {}'. \
            format(self.date_str, get_weekday(self.date_str)[1], self.open, self.close,
                   self.high, self.low, self.volume, self.turn, self.amount, self.trade_status)


class DataRt:
    FMT_RT_HEX = '<LLLfffLfLfLfLfLfLfLfLfLfLfLf'
    RT_HEX_LEN = 4*28

    def __init__(self, date, time, code, new, high, low, volume, amount,
                 b1v, b1n, b2v, b2n, b3v, b3n, b4v, b4n, b5v, b5n,
                 s1v, s1n, s2v, s2n, s3v, s3n, s4v, s4n, s5v, s5n):
            self.date, self.time = int(date), int(time)
            self.high, self.low = float(high), float(low)
            self.code, self.new = int(code), float(new)
            self.volume, self.amount = int(volume), float(amount)
            self.b1n, self.b1v, self.s1n, self.s1v = int(b1n), float(b1v), int(s1n), float(s1v)
            self.b2n, self.b2v, self.s2n, self.s2v = int(b2n), float(b2v), int(s2n), float(s2v)
            self.b3n, self.b3v, self.s3n, self.s3v = int(b3n), float(b3v), int(s3n), float(s3v)
            self.b4n, self.b4v, self.s4n, self.s4v = int(b4n), float(b4v), int(s4n), float(s4v)
            self.b5n, self.b5v, self.s5n, self.s5v = int(b5n), float(b5v), int(s5n), float(s5v)

    def __str__(self):
        return (
                '{}{:06d}  {:06d}  {:5.2f} {:5.2f} {:5.2f} {:7d} {:11.1f} | ' + ' {:5.2f} {:5d}'*5 + ' |' + ' {:5.2f} {:5d}'*5). \
            format(self.date, self.time, self.code, self.new, self.high, self.low, self.volume//100, self.amount,
                   self.b5v, self.b5n//100, self.b4v, self.b4n//100, self.b3v, self.b3n//100, self.b2v, self.b2n//100, self.b1v, self.b1n//100,
                   self.s1v, self.s1n//100, self.s2v, self.s2n//100, self.s3v, self.s3n//100, self.s4v, self.s4n//100, self.s5v, self.s5n//100)
            # '{}{:06d}  {:06d}  {:5.2f} {:5.2f} {} {:5.2f} {}'). \
            # format(self.date, self.time, self.code, self.new, self.b1v, self.b1n, self.s1v, self.s1n)


class StockRtData:
    ST_UNSUBCRIBE = 0
    ST_SUBCRIBED = 1
    status = ST_UNSUBCRIBE
    last_rec = {}
    sub_cb = None  # func(DataRt, value_change, volume_change, user_param)
    interval = None
    param = None

    @staticmethod
    def _sub_cb(result_data):
        if StockRtData.status == StockRtData.ST_UNSUBCRIBE:
            return
        for rt in result_data:
            if rt.code not in StockRtData.last_rec or rt.new != StockRtData.last_rec[rt.code].new:
                StockRtData.last_rec[rt.code] = rt
                StockRtData.sub_cb(rt, True, True, StockRtData.param)
            elif StockRtData.is_5change(rt, StockRtData.last_rec[rt.code]):
                StockRtData.sub_cb(rt, False, True, StockRtData.param)
            else:
                StockRtData.sub_cb(rt, False, False, StockRtData.param)

    @staticmethod
    def get(stocks_list):
        res = []
        try:
            r = requests.get('http://hq.sinajs.cn/?list={}'.format(','.join(stocks_list)))
            ret = r.content.decode(encoding='gbk')
            for line in ret.strip().split('\n'):
                try:
                    context = line.split('"')
                    p = context[1].split(',')
                    res.append(
                        DataRt(
                            date=p[30][2:4] + p[30][5:7] + p[30][8:10],
                            time=p[31][0:2] + p[31][3:5] + p[31][6:8],
                            code=context[0][-7:-1], new=p[3], high=p[4], low=p[5], volume=p[8], amount=p[9],
                            b1v=p[11], b1n=p[10], b2v=p[13], b2n=p[12], b3v=p[15], b3n=p[14], b4v=p[17], b4n=p[16], b5v=p[19], b5n=p[18],
                            s1v=p[21], s1n=p[20], s2v=p[23], s2n=p[22], s3v=p[25], s3n=p[24], s4v=p[27], s4n=p[26], s5v=p[29], s5n=p[28],
                        )
                    )
                except IndexError:
                    pass
        except:
            pass
        finally:
            return res

    @staticmethod
    def subscribe(sub_list, sub_cb, interval=10, measure_time=(5, 30, 0), param=None):
        assert(isinstance(sub_list, list))
        assert(sub_cb is not None)

        def _sub_get(stocks_list):
            while True:
                res = StockRtData.get(stocks_list)
                StockRtData._sub_cb(res)
                sleep(StockRtData.interval)
                if time.time() > StockRtData.finish_time:
                    break
            StockRtData.status = StockRtData.ST_UNSUBCRIBE
            StockRtData.last_rec.clear()
        if StockRtData.status is not StockRtData.ST_UNSUBCRIBE:
            print('Already subscribed.')
            return
        StockRtData.status = StockRtData.ST_SUBCRIBED
        StockRtData.sub_cb = sub_cb
        StockRtData.interval = interval
        StockRtData.finish_time = time.time() + \
                                  measure_time[0] * 3600 + measure_time[1] * 60 + measure_time[2]
        StockRtData.param = param
        th = Thread(target=_sub_get, args=[sub_list])
        th.start()
        th.join()

    @staticmethod
    def is_5change(rt1, rt2):
        assert (isinstance(rt1, DataRt) and isinstance(rt2, DataRt))
        return not (rt1.b1n, rt1.b1v, rt1.s1n, rt1.s1v) == (rt2.b1n, rt2.b1v, rt2.s1n, rt2.s1v) and \
                   (rt1.b2n, rt1.b2v, rt1.s2n, rt1.s2v) == (rt2.b2n, rt2.b2v, rt2.s2n, rt2.s2v) and \
                   (rt1.b3n, rt1.b3v, rt1.s3n, rt1.s3v) == (rt2.b3n, rt2.b3v, rt2.s3n, rt2.s3v) and \
                   (rt1.b4n, rt1.b4v, rt1.s4n, rt1.s4v) == (rt2.b4n, rt2.b4v, rt2.s4n, rt2.s4v) and \
                   (rt1.b5n, rt1.b5v, rt1.s5n, rt1.s5v) == (rt2.b5n, rt2.b5v, rt2.s5n, rt2.s5v)

    @staticmethod
    def to_csv(stock_list, callback=None, param=None, interval=10, measure_time=(5, 30, 0),):
        stock_list = [StockBasicInfo.code2sina(s) for s in stock_list]
        cw = {}

        def rec_cb(r, value_change, volume_change, user_param):
            assert(isinstance(r, DataRt))
            if value_change or volume_change:
                if r.code not in cw:
                    file = open('{}{:06d}.csv'.format(r.date, r.code), 'a', newline='')
                    cw[r.code] = (file, csv.writer(file, dialect='excel'))
                data = [r.time, r.new, r.high, r.low, r.volume, r.amount,
                        r.b5v, r.b5n, r.b4v, r.b4n, r.b3v, r.b3n, r.b2v, r.b2n, r.b1v, r.b1n,
                        r.s1v, r.s1n, r.s2v, r.s2n, r.s3v, r.s3n, r.s4v, r.s4n, r.s5v, r.s5n]
                cw[r.code][1].writerow(data)
                cw[r.code][0].flush()
                if callback is not None:
                    callback(r, value_change, volume_change, user_param)
        StockRtData.subscribe(stock_list, rec_cb, interval, measure_time, param=param)


class StockUpdateRecord:
    def __init__(self, code_name):
        self.code_name = code_name

    def update_kd(self):
        path = UtilsConfig.get_stock_data_path(self.code_name, stock_type='kd')
        if path is not None:
            if os.path.isfile(path) and os.path.getsize(path) > DataD.HEX_LEN:
                try:
                    file = open(path, 'rb')
                    file.seek(-DataD.HEX_LEN, 2)  # from the end of file
                    date_hex = file.read(4)
                    date_str = str(unpack('<L', date_hex)[0])
                    file.close()
                except Exception as e:
                    print(' ' + str(e))
                    date_str = StockData.NEW_FILE_DATE
            else:
                date_str = StockData.NEW_FILE_DATE
            date_now = datetime.datetime.now()
            date_record = datetime.datetime(int('20' + date_str[0:2]), int(date_str[2:4]), int(date_str[4:6]))
            if (date_now - date_record).days > 0:
                file = open(path, 'ab')
                kd_list = Stock.query_hist_kd(
                    self.code_name, start_date='20{}-{}-{}'.format(date_str[0:2], date_str[2:4], date_str[4:6]))
                for item in kd_list:
                    kd = DataD(*item)
                    if int(date_str) < int(kd.date_str):
                        file.write(pack(DataD.FMT_HEX,
                                        # date, open, close, high, low, unused, volume, amount,
                                        int(kd.date_str), kd.open, kd.close, kd.high, kd.low, 0, kd.volume, kd.amount,
                                        # adjustflag, turn, tradestatus, pctChg, peTTM,
                                        int(kd.adjust_flag), kd.turn, int(kd.trade_status), kd.pctChg, kd.peTTM,
                                        # psTTM, pcfNcfTTM, pbMRQ, isST
                                        kd.psTTM, kd.pcfNcfTTM, kd.pbMRQ, int(kd.isST)))
                file.close()
        else:
            print('Get path failed.')

    def update_k5(self):
        path = UtilsConfig.get_stock_data_path(self.code_name, stock_type='k5')
        if path is not None:
            if os.path.isfile(path) and os.path.getsize(path) > Data5.HEX_LEN:
                try:
                    file = open(path, 'rb')
                    file.seek(-Data5.HEX_LEN, 2)  # from the end of file
                    date_hex = file.read(4)
                    date_str = str(unpack('<L', date_hex)[0] // 10000)
                    file.close()
                except Exception as e:
                    print(' ' + str(e))
                    date_str = StockData.NEW_FILE_DATE
            else:
                date_str = StockData.NEW_FILE_DATE
            date_now = datetime.datetime.now()
            date_record = datetime.datetime(int('20' + date_str[0:2]), int(date_str[2:4]), int(date_str[4:6]))
            if (date_now - date_record).days > 0:
                file = open(path, 'ab')
                k5_list = Stock.query_hist_k5(
                    self.code_name, start_date='20{}-{}-{}'.format(date_str[0:2], date_str[2:4], date_str[4:6]))
                k5_list.sort(key=lambda i: int(i[0]))
                for item in k5_list:
                    k5 = Data5(*item)
                    if int(date_str) < int(k5.time_str[:6]):
                        #  time, open, close, high, low, volume, amount
                        file.write(pack(Data5.FMT_HEX, int(k5.time_str),
                                        k5.open, k5.close, k5.high, k5.low, k5.volume, k5.amount))
                file.close()
        else:
            print('Get path failed.')


if __name__ == '__main__':
    ssi = StocksSuperiorInfo(stocks_type=StocksSuperiorInfo.TYPE_ZZ500)
    sbi = StocksBasicInfo()
    sii = StocksIndustryInfo()
    Stock.login()
    # ssi.load_from_server()
    # sbi.load_from_server()
    # sii.load_from_server()
    Stock.logout()
    # sbi.save_to_file()
    # sbi.load_from_file()
    # sbi.dump()
    # ssi.save_to_file()
    # ssi.load_from_file()
    # ssi.dump()
    # sii.save_to_file()
    sii.load_from_file()
    sii.dump()
