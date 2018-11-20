import os
import configparser
from platform import system as system_name


__all__ = [
    'UtilsConfig'
]


class UtilsConfig:
    CONFIG_NAME = 'config.ini'
    DEFAULT_FILE_NAME = 'default.list'
    PLATFORM_WIN = 'WINDOWS'
    PLATFORM_LINUX = 'LINUX'
    FILE_ENCODING = 'FILE_ENCODING'
    PATH_BASE_PATH = 'BASE_PATH'
    PATH_DATABASE = 'DATABASE'
    PATH_STOCK_LIST = 'STOCK_LIST'
    DATA_NAME_FORMAT = 'DATA_NAME_FORMAT'

    DEFAULT_FILE_ENCODING_WIN = ''
    DEFAULT_FILE_ENCODING_LINUX = 'utf-8'
    DEFAULT_BASE_PATH_WIN = 'E:\\store\\stock'
    DEFAULT_BASE_PATH_LINUX = '/home/user/stock'
    DEFAULT_DATABASE_PATH_WIN = '\\database'
    DEFAULT_DATABASE_PATH_LINUX = DEFAULT_DATABASE_PATH_WIN.replace('\\', '/')
    DEFAULT_STOCK_LIST_PATH_WIN = '\\list'
    DEFAULT_STOCK_LIST_PATH_LINUX = DEFAULT_STOCK_LIST_PATH_WIN.replace('\\', '/')
    DEFAULT_DATA_NAME_FORMAT_WIN = '\\{}.{}.dat'
    DEFAULT_DATA_NAME_FORMAT_LINUX = DEFAULT_DATA_NAME_FORMAT_WIN.replace('\\', '/')

    DEFAULT_CONFIG = [
        # Section         Key               Value
        (PLATFORM_WIN,   FILE_ENCODING,    DEFAULT_FILE_ENCODING_WIN),
        (PLATFORM_LINUX, FILE_ENCODING,    DEFAULT_FILE_ENCODING_LINUX),
        (PLATFORM_WIN,   PATH_BASE_PATH,   DEFAULT_BASE_PATH_WIN),
        (PLATFORM_LINUX, PATH_BASE_PATH,   DEFAULT_BASE_PATH_LINUX),
        (PLATFORM_WIN,   PATH_DATABASE,    DEFAULT_DATABASE_PATH_WIN),
        (PLATFORM_LINUX, PATH_DATABASE,    DEFAULT_DATABASE_PATH_LINUX),
        (PLATFORM_WIN,   PATH_STOCK_LIST,  DEFAULT_STOCK_LIST_PATH_WIN),
        (PLATFORM_LINUX, PATH_STOCK_LIST,  DEFAULT_STOCK_LIST_PATH_LINUX),
        (PLATFORM_WIN,   DATA_NAME_FORMAT, DEFAULT_DATA_NAME_FORMAT_WIN),
        (PLATFORM_LINUX, DATA_NAME_FORMAT, DEFAULT_DATA_NAME_FORMAT_LINUX),
    ]

    m_check_config = False
    m_is_windows = None
    m_get_encoding = -1
    m_get_stock_list_path = None
    m_get_stock_data_path = None

    @staticmethod
    def check_config():
        if UtilsConfig.m_check_config:
            return
        if not os.path.isfile(UtilsConfig.CONFIG_NAME):
            UtilsConfig.create_default_config()
        UtilsConfig.m_check_config = True

    @staticmethod
    def is_windows():
        if UtilsConfig.m_is_windows is not None:
            return UtilsConfig.m_is_windows
        UtilsConfig.m_is_windows = system_name().upper() == UtilsConfig.PLATFORM_WIN
        return UtilsConfig.m_is_windows

    @staticmethod
    def create_default_config():
        print('Configuration missing, create a new one...')
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        for item in UtilsConfig.DEFAULT_CONFIG:
            try:
                cfg.add_section(item[0])
            except configparser.DuplicateSectionError:
                pass
            cfg.set(item[0], item[1], item[2])
        try:
            with open(UtilsConfig.CONFIG_NAME, 'w', encoding="utf-8") as cfg_file:
                cfg.write(cfg_file)
            print('File config.ini created.')
        except Exception as e:
            print('File create failed. '+str(e))

    @staticmethod
    def get_encoding():
        if UtilsConfig.m_get_encoding != -1:
            return UtilsConfig.m_get_encoding
        UtilsConfig.check_config()
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read(UtilsConfig.CONFIG_NAME)
        try:
            if UtilsConfig.is_windows():
                res = cfg[UtilsConfig.PLATFORM_WIN][UtilsConfig.FILE_ENCODING]
            else:
                res = cfg[UtilsConfig.PLATFORM_LINUX][UtilsConfig.FILE_ENCODING]
        except configparser.Error:
            print('Read config error')
            return None
        UtilsConfig.m_get_encoding = res if res is not None else None
        return UtilsConfig.m_get_encoding

    @staticmethod
    def get_stock_list_path(list_name=DEFAULT_FILE_NAME):
        if UtilsConfig.m_get_stock_list_path is not None:
            return UtilsConfig.m_get_stock_list_path + list_name
        UtilsConfig.check_config()
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read(UtilsConfig.CONFIG_NAME)
        try:
            if UtilsConfig.is_windows():
                path = cfg[UtilsConfig.PLATFORM_WIN][UtilsConfig.PATH_BASE_PATH] + \
                       cfg[UtilsConfig.PLATFORM_WIN][UtilsConfig.PATH_STOCK_LIST] + '\\'
            else:
                path = cfg[UtilsConfig.PLATFORM_LINUX][UtilsConfig.PATH_BASE_PATH] + \
                       cfg[UtilsConfig.PLATFORM_LINUX][UtilsConfig.PATH_STOCK_LIST] + '/'
        except configparser.Error:
            print('Read config error')
            return None
        UtilsConfig.m_get_stock_list_path = path
        return path + list_name

    @staticmethod
    def get_stock_data_path(code_name, stock_type='k5'):
        if UtilsConfig.m_get_stock_data_path is not None:
            return UtilsConfig.m_get_stock_data_path.format(code_name, stock_type)
        UtilsConfig.check_config()
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read(UtilsConfig.CONFIG_NAME)
        try:
            if UtilsConfig.is_windows():
                path = cfg[UtilsConfig.PLATFORM_WIN][UtilsConfig.PATH_BASE_PATH] + \
                       cfg[UtilsConfig.PLATFORM_WIN][UtilsConfig.PATH_DATABASE] + \
                       cfg[UtilsConfig.PLATFORM_WIN][UtilsConfig.DATA_NAME_FORMAT]
            else:
                path = cfg[UtilsConfig.PLATFORM_LINUX][UtilsConfig.PATH_BASE_PATH] + \
                       cfg[UtilsConfig.PLATFORM_LINUX][UtilsConfig.PATH_DATABASE] + \
                       cfg[UtilsConfig.PLATFORM_LINUX][UtilsConfig.DATA_NAME_FORMAT]
        except configparser.Error:
            print('Read config error')
            return None
        UtilsConfig.m_get_stock_data_path = path
        return path.format(code_name, stock_type)
