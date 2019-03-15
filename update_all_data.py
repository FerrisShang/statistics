from utils_data import *
from utils_baostock import BaoStock
import concurrent.futures
import time


__all__ = [
    'update_data',
    'update_all_data'
]


def update_record(code, update_kd, update_k5, i, item_num):
    sur = StockUpdateRecord(code)
    if update_kd:
        sur.update_kd()
        print('\r({}/{}) {} Update kd finished    '.format(
            i, item_num, code), end='', flush=True)
    if update_k5:
        sur.update_k5()
        print('\r({}/{}) {} Update k5 finished    '.format(
            i, item_num, code), end='', flush=True)


def update_data(stock_code, update_kd=True, update_k5=True):
    BaoStock.login()
    sur = StockUpdateRecord(StockBasicInfo.code2bao(stock_code))
    if update_kd:
        sur.update_kd()
        print(stock_code, 'Update kd finished' + ' ' * 40)
    if update_k5:
        sur.update_k5()
        print(stock_code, 'Update k5 finished' + ' ' * 40)
    BaoStock.logout()


def update_all_data(update_kd=False, update_k5=False):
    sbi = StocksBasicInfo()
    if not sbi.load_from_file('stock_update.list'):
        return
    item_list = sbi.get_list()
    item_num = len(item_list)
    BaoStock.login()
    if False:
        for i in range(item_num):
            assert(isinstance(item_list[i], StockBasicInfo))
            if item_list[i].status is StockStatus.DELISTING:
                continue
            sur = StockUpdateRecord(item_list[i].code)
            if update_kd:
                sur.update_kd()
                print('\r({}/{}) {} Update kd finished    '.format(
                    i, item_num, item_list[i].code), end='', flush=True)
            if update_k5:
                sur.update_k5()
                print('\r({}/{}) {} Update k5 finished    '.format(
                    i, item_num, item_list[i].code), end='', flush=True)
    else:
        with concurrent.futures.ThreadPoolExecutor(1) as executor:
            for i in range(item_num):
                assert(isinstance(item_list[i], StockBasicInfo))
                if item_list[i].status is StockStatus.DELISTING:
                    continue
                executor.submit(update_record, item_list[i].code, update_kd, update_k5, i, item_num)
    print('\rUpdate finished.' + ' ' * 40)
    BaoStock.logout()


if __name__ == '__main__':
    s = time.time()
    update_all_data(True, True)
    print(time.time()-s)
