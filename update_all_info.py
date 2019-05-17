from utils_data import *
from utils_config import UtilsConfig
from utils_baostock import Stock


__all__ = [
    'update_all_info',
    'update_recent_nmc',
    'update_report'
]


def update_all_info():
    sbi = StocksBasicInfo()
    sii = StocksIndustryInfo()
    ssi_sz50 = StocksSuperiorInfo(stocks_type=StocksSuperiorInfo.TYPE_SZ50)
    ssi_hz300 = StocksSuperiorInfo(stocks_type=StocksSuperiorInfo.TYPE_HZ300)
    ssi_zz500 = StocksSuperiorInfo(stocks_type=StocksSuperiorInfo.TYPE_ZZ500)
    # load data from server
    Stock.login()
    print('Loading all info from server.')
    sbi.load_from_server()
    sii.load_from_server()
    ssi_sz50.load_from_server()
    ssi_hz300.load_from_server()
    ssi_zz500.load_from_server()
    print('All info download success.   ')
    Stock.logout()
    # save data to local
    sbi.save_to_file('stock_all.list')
    sii.save_to_file()
    ssi_sz50.save_to_file()
    ssi_hz300.save_to_file()
    ssi_zz500.save_to_file()
    # convert industry & superior to stocks info and save
    sbi_sz50 = StocksBasicInfo()
    ind_to_info(sbi_sz50, ssi_sz50, sbi)
    sbi_sz50.save_to_file(file_name='stock_sz50.list')
    sbi_hz300 = StocksBasicInfo()
    ind_to_info(sbi_hz300, ssi_hz300, sbi)
    sbi_hz300.save_to_file(file_name='stock_hz300.list')
    sbi_zz500 = StocksBasicInfo()
    ind_to_info(sbi_zz500, ssi_zz500, sbi)
    sbi_zz500.save_to_file(file_name='stock_zz500.list')
    print('All info saved success.')


def ind_to_info(info_ind, ind, info_all):
    assert(isinstance(info_ind, StocksBasicInfo))
    assert(isinstance(ind, StocksSuperiorInfo))
    assert(isinstance(info_all, StocksBasicInfo))
    all_dict = info_all.get_dict()
    for k, v in ind.get_dict().items():
        if k in all_dict:
            info_ind.add_instance(all_dict[k])
        else:
            print('key {} not in stock list')


def update_recent_nmc(file_name='v_nmc.db'):
    path = UtilsConfig.get_stock_list_path(file_name)
    if path is None:
        return
    nmc = StockRtData.get_recent_all_nmc()
    EnvParam(path).put(nmc).save()


def update_report(file_name='v_report.db'):
    path = UtilsConfig.get_stock_list_path(file_name)
    if path is None:
        return
    env = EnvParam(path)
    report = env.get()
    report = {} if report is None else report
    last_rec = None
    for y in range(2010, 2099):
        for q in range(1, 5):
            if (y, q) not in report:
                req = StockRtData.get_report_data(y, q)
                if req is None or len(req) == 0:
                    if last_rec is not None:
                        req = StockRtData.get_report_data(*last_rec)
                        report[last_rec] = req
                    env.put(report).save()
                    return
                report[(y, q)] = req
                last_rec = (y, q)


if __name__ == '__main__':
    update_report()
    update_recent_nmc()