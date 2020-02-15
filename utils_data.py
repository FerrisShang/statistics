# -*- coding:utf-8 -*-
import os
import re
import json
import datetime
import requests
import random
import time
import csv
from collections import OrderedDict
from utils_config import UtilsConfig
from enum import IntEnum
from struct import pack, unpack
from copy import deepcopy
from threading import Thread
from pinyin import *
from time import sleep, strftime
import pickle
# from utils_tushare import *
from utils_baostock import *

__all__ = [
    'Data5',
    'DataD',
    'DataRt',
    'DataReport',
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
    'DataKzz',
    'DataKzzD',
    'StockUpdateRecord',
    'get_weekday',
    'html_get_tables',
    'Stock',
    'EnvParam',
    'Date',
]


def get_weekday(date):
    week_str = ['一', '二', '三', '四', '五', '六', '日']
    if isinstance(date, str):
        date = datetime.datetime(int(date[0:2]), int(date[2:4]), int(date[4:6]))
    else:
        date = datetime.datetime(date//10000 % 100, date//100 % 100, date % 100)
    return date.weekday(), week_str[date.weekday()]


def html_get_tables(html_text):
    tables = []
    tbs = re.compile(r'<table.*?>.*?</table>', re.DOTALL).findall(html_text)
    for tb in tbs:
        trs = re.compile(r'<tr.*?>.*?</tr>', re.DOTALL).findall(tb)
        table = []
        for tr in trs:
            tds = re.compile(r'<td.*?>.*?</td>', re.DOTALL).findall(tr)
            in_mark = False
            line = []
            for td in tds:
                td_str = ''
                for c in td:
                    if c == '<':
                        in_mark = True
                    elif c == '>':
                        in_mark = False
                    else:
                        if not in_mark:
                            td_str += c
                line.append(td_str)
            table.append(line)
        if len(table) > 0:
            tables.append(table)
    return tables


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
    area = {'3': 'sz', '0': 'sz', '6': 'sh', '1': 'sz', '5': 'sh'}

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
        assert(not sync)
        self.code = code
        self.kd_list = []
        self.k5_list = []
        self.sync = sync
        self.sync_len = 0
        load_kd = load_k5 = max(load_k5, load_kd)
        close_set = set()
        if load_kd > 0:
            self.load_kd(load_kd, start_date, end_date)
            for i in range(len(self.kd_list) - 1, -1, -1):
                if self.kd_list[i].trade_status != StockTradeStatus.ON:
                    close_set.add(self.kd_list[i].date_num)
                    del(self.kd_list[i])
        if load_k5 > 0:
            self.load_k5(load_k5, start_date, end_date)
            for i in range(len(self.k5_list) - 1, -1, -1):
                if self.k5_list[i][0].date_num in close_set or self.k5_list[i][0].open < 0.1:
                    del(self.k5_list[i])
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

    @staticmethod
    def check_update_failed(filename='stock_update.list'):
        sbi = StocksBasicInfo()
        sbi.load_from_file(filename)
        update_list = []
        for s in sbi.get_list():
            assert(isinstance(s, StockBasicInfo))
            sd = StockData(s.code, 1, 1)
            update_list.append((
                sd.code,
                sd.kd_list[-1].date_num if len(sd.kd_list) > 0 else 0,
                999999 if s.type == StockType.INDEX else sd.k5_list[-1][-1].date_num if len(sd.k5_list) > 0 else 0,
                s.code_name, s.type
            ))
        if len(update_list) > 0:
            thrust_date_str = str(datetime.datetime.today().date() + datetime.timedelta(-90))
            thrust_date = int(thrust_date_str[2:4]+thrust_date_str[5:7]+thrust_date_str[8:10])
            kd_max_date = max([i[1] for i in update_list])
            k5_max_date = max([i[2] for i in update_list if i[4] == StockType.STOCK])
            print(kd_max_date, k5_max_date, thrust_date, int(strftime('%Y%m%d')[2:]), )
            k5_m, kd_m, both_m = (0, 0, 0)
            for item in update_list:
                if item[1] < thrust_date and item[2] < thrust_date:
                    both_m += 1
                    print('Both miss: {} {} {} {}'.format(item[0], item[3], item[1], item[2]))
                elif item[1] < thrust_date:
                    print('Kd miss: {} {} {} {}'.format(item[0], item[3], item[1], item[2]))
                    kd_m += 1
                elif item[2] < thrust_date:
                    print('K5 miss: {} {} {} {}'.format(item[0], item[3], item[1], item[2]))
                    k5_m += 1
                else:
                    if item[1] < kd_max_date:
                        print('Warning(Kd short): {} {} {} {}'.format(item[0], item[3], item[1], item[2]))
                    elif item[2] < k5_max_date:
                        print('Warning(K5 short): {} {} {} {}'.format(item[0], item[3], item[1], item[2]))
                    continue
                sbi.remove(item[0])
            print('kd miss: {}, k5 miss: {}, both miss: {}'.format(kd_m, k5_m, both_m))
            sbi.save_to_file('stock_new.list')


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

    def __init__(self, date, time, code, open, pre_close, new, high, low, volume, amount,
                 b1v, b1n, b2v, b2n, b3v, b3n, b4v, b4n, b5v, b5n,
                 s1v, s1n, s2v, s2n, s3v, s3n, s4v, s4n, s5v, s5n):
            self.date, self.time = int(date), int(time)
            self.open, self.pre_close = float(open), float(pre_close)
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


class DataReport:
    header = '股票代码  每股收益(元)  每股收益同比(%)  每股净资产(元)  净资产收益率(%)  每股现金流量(元)  净利润(万元)  净利润同比(%)  发布日期  分配方案'
    def __init__(self, code, eps, epsyoy, naps, roe, cfps, np, npyoy, dp, date, year, quarter):
        """
        :param code: 股票代码
        :param eps: 每股收益(元)
        :param epsyoy: 每股收益同比(%)
        :param naps: 每股净资产(元)
        :param roe: 净资产收益率(%)
        :param cfps: 每股现金流量(元)
        :param np: 净利润(万元)
        :param npyoy: 净利润同比(%)
        :param dp: 分配方案
        """
        ps = 'eps', 'epsyoy', 'naps', 'roe', 'cfps', 'np', 'npyoy'
        try: self.code = int(code)
        except: self.code = 0
        for p in ps:
            try:
                exec('self.{} = float({})'.format(p, p))
            except:
                exec('self.{} = None'.format(p, p))
        try:
            self.dp = dp
        except:
            self.dp = None
        try:
            m, d = map(int, date.split('-'))
            self.date = ((year % 100) + (1 if quarter == 4 and m < 6 else 0)) * 10000 + m * 100 + d
        except:
            self.date = None

    def __str__(self):
        return '{}      {}       {}      {}       {}      {}    {}    {}   {}    {}'.format(
            ('%06d' % self.code),
            ('  None' if self.eps is None else '%6.2f' % self.eps),
            ('    None' if self.epsyoy is None else '%8.2f' % self.epsyoy),
            ('    None' if self.naps is None else '%8.2f' % self.naps),
            ('    None' if self.roe is None else '%8.2f' % self.roe),
            ('    None' if self.cfps is None else '%8.2f' % self.cfps),
            ('        None' if self.np is None else '%12.2f' % self.np),
            ('     None' if self.npyoy is None else '%9.2f' % self.npyoy),
            ('    None' if self.date is None else '%04d' % self.date),
            self.dp
        )


class DataKzz:
    header = '债券代码  申购代码  正股代码  正股价  转股价  债现价   发行总量 发行时间  上市时间  债券简称'

    def __init__(self, BONDCODE, SNAME, MEMO, CORRESCODE, SWAPSCODE, ZGJ_HQ, SWAPPRICE, ZQNEW, AISSUEVOL, STARTDATE, LISTDATE):

        (self.BONDCODE, self.SNAME, self.MEMO, self.CORRESCODE, self.SWAPSCODE, self.ZGJ_HQ, self.SWAPPRICE, self.ZQNEW, self.AISSUEVOL, self.STARTDATE, self.LISTDATE) = \
            0, '', '', 0, 0, -1, -1, -1, -1, -1, -1
        self.SNAME = SNAME
        self.MEMO = MEMO
        try:
            self.BONDCODE = int(BONDCODE)
        except: pass
        try:
            self.CORRESCODE = int(CORRESCODE)
        except: pass
        try:
            self.SWAPSCODE = int(SWAPSCODE)
        except: pass
        try:
            self.STARTDATE = int(STARTDATE[2:4] + STARTDATE[5:7] + STARTDATE[8:10])
        except: pass
        try:
            self.LISTDATE = int(LISTDATE[2:4] + LISTDATE[5:7] + LISTDATE[8:10])
        except: pass
        try:
            self.SWAPPRICE = float(SWAPPRICE)
        except: pass
        try:
            self.ZGJ_HQ = float(ZGJ_HQ)
        except: pass
        try:
            self.ZQNEW = float(ZQNEW)
        except: pass
        try:
            self.AISSUEVOL = float(AISSUEVOL)
        except: pass

    def __str__(self):
        return '{:06d}   {:06d}   {:06d}  {:6.2f} {:6.2f}  {:6.2f}  {:6.2f}  {:06d}   {:06d}   {}' \
            .format(
                # self.BONDCODE, self.CORRESCODE, self.SWAPSCODE, self.ZGJ_HQ, self.SWAPPRICE, self.STARTDATE, self.LISTDATE, self.SNAME
                self.BONDCODE, self.CORRESCODE, self.SWAPSCODE, self.ZGJ_HQ, self.SWAPPRICE, self.ZQNEW, self.AISSUEVOL, self.STARTDATE, self.LISTDATE, self.SNAME
        )


class DataKzzD:
    header = '  日期    收盘价   纯债值   转股值   债溢价   股溢价   转股价   剩余份额'

    def __init__(self, date, fclose, purebondvalue, swapvalue, swapor, purebondor, swapprice, syfe):
        try:
            t = datetime.datetime.fromtimestamp(int(date)//1000)
            self.date = int(t.strftime('%Y%m%d')) % 1000000
        except: self.date = 0
        try:
            self.fclose = float(fclose)
        except: self.fclose = -1
        try:
            self.purebondvalue = float(purebondvalue)
        except: self.purebondvalue = -1
        try:
            self.swapvalue = float(swapvalue)
        except: self.swapvalue = -1
        try:
            self.swapor = float(swapor)
        except: self.swapor = 0
        try:
            self.purebondor = float(purebondor)
        except: self.purebondor = 0
        try:
            self.swapprice = float(swapprice)
        except: self.swapprice = None
        try:
            self.syfe = float(syfe)
        except: self.syfe = None

    def __str__(self):
        return '{:06d}   {:6.2f}  {:6.2f}  {:6.2f}  {:6.2f}  {:6.2f}  {}  {}'.format(
            self.date, self.fclose, self.purebondor, self.swapvalue, self.swapor, self.purebondor,
            '  -   ' if self.swapprice is None else '%6.2f' % self.swapprice,
            ' -' if self.syfe is None else '%6.0f' % self.syfe
        )

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
        for i in range(0, len(stocks_list), 256):
            try:
                r = requests.get('http://hq.sinajs.cn/?list={}'.format(','.join(stocks_list[i:min(len(stocks_list), i+256)])))
                ret = r.content.decode(encoding='gbk')
                for line in ret.strip().split('\n'):
                    try:
                        context = line.split('"')
                        p = context[1].split(',')
                        res.append(
                            DataRt(
                                date=p[30][2:4] + p[30][5:7] + p[30][8:10],
                                time=p[31][0:2] + p[31][3:5] + p[31][6:8],
                                open=p[1], pre_close=p[2],
                                code=context[0][-7:-1], new=p[3], high=p[4], low=p[5], volume=p[8], amount=p[9],
                                b1v=p[11], b1n=p[10], b2v=p[13], b2n=p[12], b3v=p[15], b3n=p[14], b4v=p[17], b4n=p[16], b5v=p[19], b5n=p[18],
                                s1v=p[21], s1n=p[20], s2v=p[23], s2n=p[22], s3v=p[25], s3n=p[24], s4v=p[27], s4n=p[26], s5v=p[29], s5n=p[28],
                            )
                        )
                    except IndexError:
                        pass
            except:
                pass
        return res

    @staticmethod
    def get_recent_k5(code, start=0, end=2111111111):
        KLINE_TT_MIN_URL = 'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,m5,,640&_var=m5_today&r=0.%s'
        r = str(random.randint(10**(16-1), (10**16)-1))
        res = []
        try:
            symbol = StockBasicInfo.code2sina(code)
            r = requests.get(KLINE_TT_MIN_URL % (symbol, r))
            ret = r.content.decode(encoding='utf-8')
            lines = ret.split('=')[1]
            reg = re.compile(r',{"nd.*?}')
            lines = re.subn(reg, '', lines)
            js = json.loads(lines[0])
            dataflag = 'm5'
            if len(js['data'][symbol][dataflag][0]) >= 6:
                data = js['data'][symbol][dataflag]
                for d in data:
                    if start <= int(d[0][2:12]) <= end:
                        res.append(Data5(*d[:5], float(d[5])*100, 0))
            else:
                return None
        except:
            pass
        finally:
            return res

    @staticmethod
    def get_ndrr(code):
        try:
            code = code if isinstance(code, int) else int(code[-6:])
            url = 'http://hq.stock.sohu.com/cn/%03d/cn_%06d-1.html'
            r = requests.get(url % (code % 1000, code))
            html_text = r.content.decode(encoding='gbk')
            data = re.compile(r'\[\'cn_{:06d}.*?\'\]'.format(code), re.DOTALL).findall(html_text)[0]
            data = data.replace("'", "")[1:-1].split(',')
            return data[1], float(data[2])
        except:
            return None

    @staticmethod
    def get_all_ndrr(type='sz'):
        sz_codes = [
            131810, 131811, 131800, 131809, 131801, 131802, 131803, 131805, 131806,
        ]
        sh_codes = [
            204001, 204002, 204003, 204004, 204007, 204014, 204028, 204091, 204182,
        ]
        if type == 'sz':
            codes = sz_codes
        elif type == 'sh':
            codes = sh_codes
        else:
            codes = sz_codes + sh_codes
        result = []
        for code in codes:
            r = StockRtData.get_ndrr(code)
            if r is not None:
                result.append(r)
        return result

    @staticmethod
    def get_recent_all_nmc():
        url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=80&sort=nmc&asc=0&node=hs_a&symbol=&_s_r_a=page&page=%d'
        res = {}
        for i in range(50):
            try:
                r = requests.get(url%i)
                ret = r.content.decode(encoding='gbk')
                if len(ret) < 128:
                    break
                reg = re.compile(r',(.*?):')
                text = reg.sub(r',"\1":', ret).replace('"{symbol', '{"symbol').replace('{symbol', '{"symbol"')
                js = json.loads(text)
                for j in js:
                    res[int(j['code'])] = round(j['nmc'] / 10000)
            except:
                pass
        return res


    @staticmethod
    def get_report_data(year, quarter, retry_count=3):
        def check_header(h):
            name_list = ['股票代码', '股票名称', '每股收益', '每股收益同比', '每股净资产(元)', '净资产收益率(%)',
                         '每股现金流量(元)', '净利润(万元)', '净利润同比(%)', '分配方案', '发布日期', '详细']
            if len(h) == len(name_list) and min([ name in item for name, item in zip(name_list, h) ]):
                return True
        url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/mainindex/index.phtml?s_i=&s_a=&s_c=&reportdate=%d&quarter=%d&num=10000'
        res = {}
        for _ in range(retry_count):
            time.sleep(0.01)
            try:
                r = requests.get(url%(year, quarter))
                html_text = r.content.decode(encoding='gbk')
                if len(html_text) < 512:
                    break
                tables = html_get_tables(html_text)
                if len(tables) != 1 or len(tables[0]) < 2 or not check_header(tables[0][0]):
                    return None
                for line in tables[0][1:]:
                    try:
                        res[int(line[0])] = DataReport(line[0], *line[2:11], year, quarter)
                    except:
                        pass
                break
            except Exception as e:
                pass
        return res


    @staticmethod
    def get_all_kzz(retry_count=3):
        def decode2float(code, decode_map):
            for d in decode_map:
                code = code.replace(d[0], d[1])
            return code

        url = 'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=KZZ_LB2.0&token=70f12f2f4f091e459a279469fe49eca5&js=var%20{jsname}={data:(x),font:(font)}'
        res = []
        for _ in range(retry_count):
            time.sleep(0.01)
            try:
                html_text = requests.get(url).content.decode('utf-8')
                assert(len(html_text) > 512)
                # with open('tmp.txt', 'w') as f: f.write(html_text)
                value_map_text = re.compile(r'\"FontMapping":\[{.*?}\]', re.DOTALL).findall(html_text)
                assert(len(value_map_text) == 1)
                js = json.loads(value_map_text[0][len('"FontMapping":'):])
                decode_map = []
                for j in js:
                    decode_map.append((j['code'], str(j['value'])))

                value_data = re.compile(r'data:.*?"}],font:', re.DOTALL).findall(html_text)
                assert(len(value_data) == 1)
                js_items = json.loads(value_data[0][len('data:'):-len('",font:')+1])

                for item in js_items:
                    res.append(DataKzz(
                        item['BONDCODE'],
                        item['SNAME'],
                        item['MEMO'],
                        item['CORRESCODE'],
                        item['SWAPSCODE'],
                        decode2float(item['ZGJ_HQ'], decode_map),
                        decode2float(item['ZGJZGJ'], decode_map),
                        decode2float(item['ZQNEW'], decode_map),
                        decode2float(item['AISSUEVOL'], decode_map),
                        item['STARTDATE'],
                        item['LISTDATE'],
                    ))
                break
            except Exception as e:
                pass
        return res


    @staticmethod
    def get_kzz_kd(code, retry_count=3):
        url = 'http://gwapi.eastmoney.com/2412/data/kzz_ls?appid=1258&tk=E72CC88D2D02FBB1D5576837B70B8B35&pagesize=8000&pageindex=1&order=asc&orderby=date&zcode=%06d'
        code = code if isinstance(code, int) else int(code[-6:])
        res = []
        for _ in range(retry_count):
            time.sleep(0.01)
            try:
                html_text = requests.get(url % code).content.decode('utf-8')
                assert(len(html_text) > 512)
                # with open('tmp.txt', 'w') as f: f.write(html_text)
                js_items = json.loads(html_text)
                for j in js_items['data']:
                    res.append(DataKzzD(
                        j['date'], j['fclose'], j['purebondvalue'], j['swapvalue'], j['swapor'],
                        j['purebondor'], j['swapprice'], j['syfe']
                    ))
                break
            except Exception as e:
                pass
        return res



    @staticmethod
    def get_report_data_detail(code, retry_count=3):
        url = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/%06d/ctrl/part/displaytype/1000.phtml'
        res = {}
        for _ in range(retry_count):
            time.sleep(0.01)
            try:
                r = requests.get(url % (int(code)))
                html_text = r.content.decode(encoding='gbk')
                if len(html_text) < 512:
                    break
                tables = html_get_tables(html_text)
                if len(tables) == 0:
                    return None
                for table in tables:
                    if len(table) != 32 or (not isinstance(table[1], list) or len(table[1]) != 6 or table[1][0] != '报表日期'):
                        continue
                    print(len(table), table)
                    #TODO: list to custom data type
                break
            except Exception as e:
                pass
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
        if len(stock_list) == 0:
            return
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
                if isinstance(k5_list, list):
                    k5_list.sort(key=lambda i: int(i[0]))
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


def get_hist_data_online(code, start_time, end_time, fqt=1):
    # fqt: 0,1,2: no,front,back
    # http://68.push2his.eastmoney.com/api/qt/stock/details/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&secid=105.NDAQ&pos=-30
    KLINE_URL = 'http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={}{}&fields1=f1&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt={}&beg={}&end={}'
    res = []
    try:
        t = ''
        if len(code) == 6 and (code[0] == '5' or code[0] == '6'): t = '1.'
        elif len(code) == 6 and (code[0] == '0' or code[0] == '1' or code[0] == '3'): t = '0.'
        r = requests.get(KLINE_URL.format(t, code, fqt, start_time, end_time))
        ret = r.content.decode(encoding='utf-8')
        js = json.loads(ret)
        if len(js['data']['klines']) > 0:
            for d in js['data']['klines']:
                res.append(DataD(*d.split(','), *['0' for _ in range(9)]))
        else:
            return None
    except:
        pass
    finally:
        return res


class EnvParam:
    def __init__(self, file_name='env.db'):
        self.env = {}
        self.file_name = file_name
        self.load()

    def load(self):
        try:
            with open(self.file_name, 'rb') as f:
                self.env = pickle.load(f)
        except:
            self.env = {}
        return self

    def save(self):
        with open(self.file_name, 'wb') as f:
            pickle.dump(self.env, f)
        return self

    def get(self, param_name='default'):
        return self.env[param_name] if param_name in self.env else None

    def put(self, param, param_name='default'):
        self.env[param_name] = param
        return self

    def delete(self, param_name='default'):
        if param_name in self.env:
            del(self.env[param_name])
        return self

    def clear(self):
        self.env = {}
        return self


def get_hist_data(code, start_time, end_time, fqt=1):
    env_name = '.eastmoney.hist_data.cache'
    env = EnvParam(env_name)
    hist_name = str(code) + str(fqt)
    hist_data = env.get(hist_name)
    update_hist_flag = False
    if hist_data is None or len(hist_data) < 2:
        update_hist_flag = True
        if len(env.env) > 32:
            env.clear().save()
    elif 20000000+hist_data[0].date_num > int(start_time) or 20000000+hist_data[-1].date_num < int(end_time):
        update_hist_flag = True
        env.delete(hist_name).save()
        print('Update hist data: {} {}-{}'.format(code, start_time, end_time))
    if update_hist_flag:
        hist_data = get_hist_data_online(code,
                                         20000000+Date.get_day(str(start_time)[2:], -10),
                                         20000000+Date.get_day(str(end_time)[2:], 10),
                                         fqt)
        env.put(hist_data, hist_name).save()
    s_idx = e_idx = 0
    for i in range(len(hist_data)):
        if hist_data[i].date_num >= int(start_time):
            s_idx = i
            break
    for i in range(len(hist_data)-1, -1, -1):
        if hist_data[i].date_num <= int(end_time):
            e_idx = i
            break
    return hist_data[s_idx:e_idx]


class Date:
    @staticmethod
    def get_now():
        return int(time.strftime('%Y%m%d')[-6:])

    @staticmethod
    def get_day(now=None, diff=0):
        if now is None:
            now = Date.get_now()
        if isinstance(now, str):
            date = datetime.datetime(int(now[:2]), int(now[2:4]), int(now[4:6]))
        else:
            date = datetime.datetime(2000+now//10000, (now//100)%100, now%100)
        date += datetime.timedelta(diff)
        return int(date.strftime('%Y%m%d')[-6:])

    @staticmethod
    def get_diff(s, e):
        s = datetime.datetime(2000+s//10000, (s//100)%100, s%100)
        e = datetime.datetime(2000+e//10000, (e//100)%100, e%100)
        delta = e - s
        return delta.days

from utils import *
if __name__ == '__main__':
    res = get_hist_data('105.NDAQ', '20150101', '20200101')
    plt_plot([x.open for x in res])