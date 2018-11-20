from utils_data import *
from utils_baostock import BaoStock


__all__ = [
    'update_all_data'
]


def update_all_data(update_kd=False, update_k5=False):
    sbi = StocksBasicInfo()
    if not sbi.load_from_file('stock_update.list'):
        return
    item_list = sbi.get_list()
    item_num = len(item_list)
    BaoStock.login()
    for i in range(item_num):
        assert(isinstance(item_list[i], StockBasicInfo))
        if item_list[i].status is StockStatus.DELISTING:
            continue
        sur = StockUpdateRecord(item_list[i].code)
        if update_kd:
            sur.update_kd()
            print('\r({}/{}) {} Update kd finished'.format(
                i, item_num, item_list[i].code), end='', flush=True)
        if update_k5:
            sur.update_k5()
            print('\r({}/{}) {} Update k5 finished'.format(
                i, item_num, item_list[i].code), end='', flush=True)
    print('\rUpdate finished.')
    BaoStock.logout()


if __name__ == '__main__':
    update_all_data(True, True)
