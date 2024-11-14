# Copyright (c) vnquant. All rights reserved.
from typing import Union, Optional
import pandas as pd
import requests
from datetime import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import sys
# from vnquant import utils
# from vnquant import configs
# from vnquant.log import logger
# from vnquant.data.loader.proto import DataLoadProto

import logging
from bs4 import BeautifulSoup
import  re
from datetime import datetime


def convert_date(text, date_type = '%Y-%m-%d'):
    return datetime.strptime(text, date_type)

def convert_text_dateformat(text, origin_type = '%Y-%m-%d', new_type = '%Y-%m-%d'):
    return convert_date(text, origin_type).strftime(new_type)

def clean_text(text):
    return re.sub('[(\n\t)*]', '', text).strip()

def split_change_col(text):
    return re.sub(r'[\(|\)%]', '', text).strip().split()
    
logging.basicConfig(
     format="%(asctime)s - %(name)s - %(levelname)s \
     - %(funcName)s - line %(lineno)d - %(message)s",
     datefmt="[%Y-%m-%d %H:%M:%S]",
     force=True
)

logger = logging.getLogger("Assitant")
logger.setLevel(logging.INFO)

URL_VND = 'https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml'
API_VNDIRECT = 'https://finfo-api.vndirect.com.vn/v4/stock_prices/'
URL_CAFE = "https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx"
HEADERS = {'content-type': 'application/x-www-form-urlencoded', 'User-Agent': 'Mozilla'}
STOCK_COLUMNS=[
    'code', 'date', 'floor', 
    'basic_price', 'ceiling_price', 'floor_price',
    'close', 'open', 'high', 'low', 'average',
    'adjust_close', 'adjust_open', 'adjust_high', 'adjust_low', 'adjust_average',
    'change', 'adjust_change', 'percent_change',
    'volume_match', 'value_match', 'volume_reconcile', 'value_reconcile'
]
API_VNDIRECT = API_VNDIRECT
HEADERS = HEADERS

class DataLoadProto():
    def __init__(self, symbols: Union[str, list], start, end, *arg, **karg):
        self.symbols = symbols
        self.start = convert_text_dateformat(start, new_type='%d/%m/%Y')
        self.end = convert_text_dateformat(end, new_type='%d/%m/%Y')

    def pre_process_symbols(self):
        if isinstance(self.symbols, list):
            symbols = self.symbols
        else:
            symbols = [self.symbols]
        return symbols

STOCK_COLUMNS=[
    'code', 'date', 'floor', 
    'basic_price', 'ceiling_price', 'floor_price',
    'close', 'open', 'high', 'low', 'average',
    'adjust_close', 'adjust_open', 'adjust_high', 'adjust_low', 'adjust_average',
    'change', 'adjust_change', 'percent_change',
    'volume_match', 'value_match', 'volume_reconcile', 'value_reconcile'
]

STOCK_COLUMNS_CAFEF=[
    'code', 'date',
    'close', 'open', 'high', 'low', 'adjust_price', 'change_str',
    'volume_match', 'value_match', 'volume_reconcile', 'value_reconcile'
]

STOCK_COLUMNS_CAFEF_FINAL=[
    'code', 'date',
    'close', 'open', 'high', 'low', 'adjust_price', 'change', 'percent_change',
    'volume_match', 'value_match', 'volume_reconcile', 'value_reconcile'
]

REGEX_PATTERN_PRICE_CHANGE_CAFE = r'([-+]?\d*\.\d+|\d+)\s*\(\s*([-+]?\d*\.\d+|\d+)\s*%\s*\)'

class DataLoaderCAFE(DataLoadProto):
    def __init__(self, symbols, start, end, *arg, **karg):
        self.symbols = symbols
        self.start = start
        self.end = end
        super(DataLoaderCAFE, self).__init__(symbols, start, end)

    def download(self):
        stock_datas = []
        symbols = self.pre_process_symbols()
        logger.info('Start downloading data symbols {} from CAFEF, start: {}, end: {}!'.format(symbols, self.start, self.end))

        for symbol in symbols:
            stock_datas.append(self.download_one(symbol))

        data = pd.concat(stock_datas, axis=1)
        data = data.sort_index(ascending=False)
        return data

    def download_one(self, symbol):
        start_date = convert_text_dateformat(self.start, origin_type = '%d/%m/%Y', new_type = '%Y-%m-%d')
        end_date = convert_text_dateformat(self.end, origin_type = '%d/%m/%Y', new_type = '%Y-%m-%d')
        delta = datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')
        params = {
            "Symbol": symbol, # symbol of stock
            "StartDate": start_date, # start date
            "EndDate": end_date, # end date
            "PageIndex": 1, # page number
            "PageSize":delta.days + 1 # the size of page
        }
        # Note: We set the size of page equal to the number of days from start_date and end_date
        # and page equal to 1, so that we can get a full data in the time interval from start_date and end_date
        res = requests.get(URL_CAFE, params=params)
        data = res.json()['Data']['Data']
        if not data:
            logger.error(f"Data of the symbol {symbol} is not available")
            return None
        data = pd.DataFrame(data)
        data[['code']] = symbol
        stock_data = data[['code', 'Ngay',
                           'GiaDongCua', 'GiaMoCua', 'GiaCaoNhat', 'GiaThapNhat', 'GiaDieuChinh', 'ThayDoi',
                           'KhoiLuongKhopLenh', 'GiaTriKhopLenh', 'KLThoaThuan', 'GtThoaThuan']].copy()

        stock_data.columns = STOCK_COLUMNS_CAFEF

        stock_change = stock_data['change_str'].str.extract(REGEX_PATTERN_PRICE_CHANGE_CAFE, expand=True)
        stock_change.columns = ['change', 'percent_change']
        stock_data = pd.concat([stock_data, stock_change], axis=1)
        stock_data = stock_data[STOCK_COLUMNS_CAFEF_FINAL]

        list_numeric_columns = [
            'close', 'open', 'high', 'low', 'adjust_price',
            'change', 'percent_change',
            'volume_match', 'value_match', 'volume_reconcile', 'value_reconcile'
        ]
        
        stock_data = stock_data.set_index('date')
        stock_data[list_numeric_columns] = stock_data[list_numeric_columns].astype(float)
        stock_data.index = list(map(lambda x: datetime.strptime(x, '%d/%m/%Y'), stock_data.index))
        stock_data.index.name = 'date'
        stock_data = stock_data.sort_index(ascending=False)
        stock_data.fillna(method='ffill', inplace=True)
        stock_data['total_volume'] = stock_data.volume_match + stock_data.volume_reconcile
        stock_data['total_value'] = stock_data.value_match + stock_data.value_reconcile

        # Create multiple columns
        iterables = [stock_data.columns.tolist(), [symbol]]
        mulindex = pd.MultiIndex.from_product(iterables, names=['Attributes', 'Symbols'])
        stock_data.columns = mulindex

        logger.info('data {} from {} to {} have already cloned!' \
                     .format(symbol,
                             convert_text_dateformat(self.start, origin_type = '%d/%m/%Y', new_type = '%Y-%m-%d'),
                             convert_text_dateformat(self.end, origin_type='%d/%m/%Y', new_type='%Y-%m-%d')))
        return stock_data

def stock_wide_format(symbols=['VIC', 'VPB', 'BSC']):
    loader2 = DataLoaderCAFE(symbols=symbols, start="2024-10-10", end=datetime.today().strftime("%Y-%m-%d"))
    stock_price = loader2.download() 
    stock_price = stock_price[['adjust_price']]
    stock_price.columns = stock_price.columns.droplevel(0)
    return stock_price

def stock_long_format(symbols=['VIC', 'VPB', 'BSC']):
    loader2 = DataLoaderCAFE(symbols=symbols, start="2024-10-10", end=datetime.today().strftime("%Y-%m-%d"))
    stock_price = loader2.download() 
    stock_price = stock_price[['adjust_price']]
    stock_price.columns = stock_price.columns.droplevel(0)
    df_long = stock_price.reset_index().melt(id_vars=["date"], var_name="symbol", value_name="close")
    df_long['date'] = df_long['date'].dt.strftime('%Y-%m-%d')


    return df_long
