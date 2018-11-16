import os
import datetime
from collections import OrderedDict
from utils_config import UtilsConfig
from enum import IntEnum
from struct import pack, unpack
from utils_baostock import *


__all__ = [
    'Data5',
    'DataD',
    'StockType',
    'StockStatus',
    'StockBasicInfo',
    'StocksBasicInfo',
    'StockData',
    'StockUpdateRecord',
]


class StockType(IntEnum):
    STOCK = 1
    INDEX = 2
    OTHER = 3


class StockStatus(IntEnum):
    LISTING = 1
    DELISTING = 0


class StockBasicInfo:
    def __init__(self, code, code_name, ipo_date, out_date, type, status):
        self.key = int(code[3:])
        self.code = code
        self.code_name = code_name
        self.ipoDate = int(str(ipo_date).replace('-', ''))
        self.outDate = int(str(out_date).replace('-', '')) if out_date is not '' else 20991231
        self.type = StockType(int(type))
        self.status = StockStatus(int(status))

    def __str__(self):
        return '{} {} {} {:06d} {:06d} {}'.format(self.code, self.type.name, self.status.name, self.ipoDate, self.outDate, self.code_name)


class StocksBasicInfo:
    def __init__(self):
        self.infolist = OrderedDict()

    def load_from_file(self, file_name='default.list'):
        path = UtilsConfig.get_stock_list_path(file_name)
        if path is not None and os.path.isfile(path):
            with open(path, 'r', encoding="ISO-8859-1") as f:
                for line in f.readlines():
                    info = StockBasicInfo(*line.split())
                    self.infolist[info.key] = info
                f.close()
                return True
        else:
            print('load file failed.')
            return False

    def load_from_server(self):
        BaoStock.login()
        info_list = BaoStock.query_basic()
        BaoStock.logout()
        for line in info_list:
            info = StockBasicInfo(*line)
            self.infolist[info.key] = info
        return True

    def save_to_file(self, file_name='default.list'):
        path = UtilsConfig.get_stock_list_path(file_name)
        if path is not None:
            with open(path, 'w', encoding="ISO-8859-1") as f:
                for key, line in self.infolist.items():
                    assert(isinstance(line, StockBasicInfo))
                    f.write('{} {} {} {} {} {}'.format(line.code, line.code_name, line.ipoDate, line.outDate,
                            line.status.numerator, line.type.numerator))
                f.close()
                return True
        else:
            print('save file failed.')
            return False

    def add(self, code, code_name, ipo_date, out_date, type, status):
        info = StockBasicInfo(code, code_name, ipo_date, out_date, type, status)
        self.infolist[info.key] = info

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

    def dump(self):
        for key, value in self.infolist.items():
            print(value)


class StockData:
    FMT_K5_HEX = '<LffffLf'
    K5_HEX_LEN = 4*7
    FMT_KD_HEX = '<LffffLQfLfLfffffL'
    KD_HEX_LEN = 4*18
    NEW_FILE_DATE = '150101'

    def __init__(self, code_name, load_kd=False, load_k5=False, sync=False):
        assert(isinstance(code_name, str))
        self.code_name = code_name
        self.kd_list = []
        self.k5_list = []
        self.sync = sync
        self.sync_len = 0
        if load_kd:
            self.load_kd()
        if load_k5:
            self.load_k5()

    @staticmethod
    def parse_hex_k5(path):
        ret_list = []
        data_list = []
        size = os.path.getsize(path)
        if 0 == (size % StockData.K5_HEX_LEN):
            f = open(path, 'rb')
            for i in range(size//StockData.K5_HEX_LEN):
                item_data = f.read(StockData.K5_HEX_LEN)
                data_list.append(Data5(*unpack(StockData.FMT_K5_HEX, item_data)))
            f.close()
            if (len(data_list) % 48) != 0:  # lose part data of a day
                print('{}  {}'.format(path, len(data_list)))
            for i in range(len(data_list) // 48):
                ret_list.append(data_list[i * 48:(i+1) * 48])
            return ret_list
        else:
            print('File size error: ' + path)

    @staticmethod
    def parse_hex_kd(path):
        ret_list = []
        size = os.path.getsize(path)
        if 0 == (size % StockData.KD_HEX_LEN):
            f = open(path, 'rb')
            for i in range(size//StockData.KD_HEX_LEN):
                item_data = f.read(StockData.KD_HEX_LEN)
                params = unpack(StockData.FMT_KD_HEX, item_data)
                ret_list.append(DataD(*(params[:5]+params[6:])))  # ignore unused integer
            f.close()
            return ret_list
        else:
            print('File size error: ' + path)

    def load_kd(self):
        path = UtilsConfig.get_stock_data_path(self.code_name, stock_type='kd')
        if path is not None and os.path.isfile(path):
            self.kd_list = StockData.parse_hex_kd(path)
            return True
        else:
            print('Load kd error')
            return False

    def load_k5(self):
        path = UtilsConfig.get_stock_data_path(self.code_name, stock_type='k5')
        if path is not None and os.path.isfile(path):
            self.k5_list = StockData.parse_hex_k5(path)
            return True
        else:
            print('Load k5 error')
            return False


class Data5:
    def __init__(self, v_time, v_open, v_close, v_high, v_low, volume, amount):
        # ['20181109150000000', '18.5400', '18.5200', '18.5500', '18.5000', '99200', '1837097.0000']
        if isinstance(v_time, str):
            self.time_str = v_time[2:12]
        elif isinstance(v_time, int):
            self.time_str = str(v_time)
        else:
            assert False
        self.open = float(v_open)
        self.close = float(v_close)
        self.high = float(v_high)
        self.low = float(v_low)
        self.volume = int(volume)
        self.amount = float(amount)

    def __str__(self):
        return '{}  {:6.2f}  {:6.2f}  {:6.2f}  {:6.2f}  {:10d}  {:.2f}'. \
            format(self.time_str, self.open, self.close,
                   self.high, self.low, self.volume, self.amount)


class DataD:
    def __init__(self, date, v_open, v_close, v_high, v_low, volume, amount, adjust_flag,
                 turn, trade_status, pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST):
        # ['2018-11-01', '18.9000', '18.2200', '19.1200', '18.1100', '4301411', '79888895.9000', '3', '3.587307', '1', '1.053792', '179.076255', '16.478888', '71.301441', '3.187132', '0']
        if isinstance(date, str):
            self.date_str = date[2:4]+date[5:7]+date[8:10]
        elif isinstance(date, int):
            self.date_str = str(date)
        else:
            assert False
        self.open = float(v_open)
        self.close = float(v_close)
        self.high = float(v_high)
        self.low = float(v_low)
        self.volume = int(volume)
        self.amount = float(amount)
        self.adjust_flag = int(adjust_flag)
        self.turn = float(turn) if turn != '' else 0.0
        self.trade_status = bool(trade_status)
        self.pctChg = float(pctChg)  # 涨跌幅
        self.peTTM = float(peTTM)  # 动态市盈率
        self.psTTM = float(psTTM)  # 市销率
        self.pcfNcfTTM = float(pcfNcfTTM)  # 市现率
        self.pbMRQ = float(pbMRQ)  # 市净率
        self.isST = bool(isST)  # 是否ST

    def __str__(self):
        return '{}  {:6.2f}  {:6.2f}  {:6.2f}  {:6.2f}  {:10d}  {:5.2f}  {:.2f}  {}'. \
            format(self.date_str, self.open, self.close,
                   self.high, self.low, self.volume, self.turn, self.amount, self.trade_status)


class StockUpdateRecord:
    def __init__(self, code_name):
        self.code_name = code_name

    def update_kd(self):
        path = UtilsConfig.get_stock_data_path(self.code_name, stock_type='kd')
        if path is not None:
            if os.path.isfile(path):
                try:
                    file = open(path, 'rb')
                    file.seek(-StockData.KD_HEX_LEN, 2)  # from the end of file
                    date_hex = file.read(4)
                    date_str = str(unpack('L', date_hex)[0])
                    file.close()
                except:
                    date_str = StockData.NEW_FILE_DATE
            else:
                date_str = StockData.NEW_FILE_DATE
            date_now = datetime.datetime.now()
            date_record = datetime.datetime(int('20' + date_str[0:2]), int(date_str[2:4]), int(date_str[4:6]))
            if (date_now - date_record).days > 0:
                file = open(path, 'ab')
                kd_list = BaoStock.query_hist_kd(
                    self.code_name, start_date='20{}-{}-{}'.format(date_str[0:2], date_str[2:4], date_str[4:6]))
                for item in kd_list:
                    kd = DataD(*item)
                    if int(date_str) < int(kd.date_str):
                        file.write(pack(StockData.FMT_KD_HEX,
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
            if os.path.isfile(path):
                try:
                    file = open(path, 'rb')
                    file.seek(-StockData.K5_HEX_LEN, 2)  # from the end of file
                    date_hex = file.read(4)
                    date_str = str(unpack('L', date_hex)[0] // 10000)
                    file.close()
                except:
                    date_str = StockData.NEW_FILE_DATE
            else:
                date_str = StockData.NEW_FILE_DATE
            date_now = datetime.datetime.now()
            date_record = datetime.datetime(int('20' + date_str[0:2]), int(date_str[2:4]), int(date_str[4:6]))
            if (date_now - date_record).days > 0:
                file = open(path, 'ab')
                k5_list = BaoStock.query_hist_k5(
                    self.code_name, start_date='20{}-{}-{}'.format(date_str[0:2], date_str[2:4], date_str[4:6]))
                k5_list.sort(key=lambda i: int(i[0]))
                for item in k5_list:
                    k5 = Data5(*item)
                    if int(date_str) < int(k5.time_str[:6]):
                        #  time, open, close, high, low, volume, amount
                        file.write(pack(StockData.FMT_K5_HEX, int(k5.time_str),
                                        k5.open, k5.close, k5.high, k5.low, k5.volume, k5.amount))
                file.close()
        else:
            print('Get path failed.')


if __name__ == '__main__':
    BaoStock.login()
    stock_update = StockUpdateRecord('sh.600000')
    stock_update.update_kd()
    BaoStock.logout()
