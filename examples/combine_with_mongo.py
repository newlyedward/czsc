# coding: utf-8
import logging
import time
from datetime import datetime, timedelta

import numpy
import pandas as pd
import pymongo
from pandas import DataFrame

from czsc import KlineAnalyze

uri = 'mongodb://localhost:27017/quantaxis'
client = pymongo.MongoClient(uri)
DATABASE = client.quantaxis


def util_code_tostr(code):
    """
    explanation:
        将所有沪深股票从数字转化到6位的代码,因为有时候在csv等转换的时候,诸如 000001的股票会变成office强制转化成数字1,
        同时支持聚宽股票格式,掘金股票代码格式,Wind股票代码格式,天软股票代码格式

    params:
        * code ->
            含义: 代码
            类型: str
            参数支持: []
    """
    if isinstance(code, int):
        return "{:>06d}".format(code)
    if isinstance(code, str):
        # 聚宽股票代码格式 '600000.XSHG'
        # 掘金股票代码格式 'SHSE.600000'
        # Wind股票代码格式 '600000.SH'
        # 天软股票代码格式 'SH600000'
        code = code.upper()  # 数据库中code名称都存为大写
        if len(code) == 6:
            return code
        if len(code) == 8:
            # 天软数据
            return code[-6:]
        if len(code) == 9:
            return code[:6]
        if len(code) == 11:
            if code[0] in ["S"]:
                return code.split(".")[1]
            return code.split(".")[0]
        raise ValueError("错误的股票代码格式")
    if isinstance(code, list):
        return util_code_tostr(code[0])


def util_code_tolist(code, auto_fill=True):
    """
    explanation:
        将转换code==> list

    params:
        * code ->
            含义: 代码
            类型: str
            参数支持: []
        * auto_fill->
            含义: 是否自动补全(一般是用于股票/指数/etf等6位数,期货不适用) (default: {True})
            类型: bool
            参数支持: [True]
    """

    if isinstance(code, str):
        if auto_fill:
            return [util_code_tostr(code)]
        else:
            return [code]

    elif isinstance(code, list):
        if auto_fill:
            return [util_code_tostr(item) for item in code]
        else:
            return [item for item in code]


def util_date_valid(date):
    """
    explanation:
        判断字符串格式(1982-05-11)

    params:
        * date->
            含义: 日期
            类型: str
            参数支持: []

    return:
        bool
    """
    try:
        time.strptime(date, "%Y-%m-%d")
        return True
    except:
        return False


def util_date_stamp(date):
    """
    explanation:
        转换日期时间字符串为浮点数的时间戳

    params:
        * date->
            含义: 日期时间
            类型: str
            参数支持: []

    return:
        time
    """
    datestr = str(date)[0:10]
    date = time.mktime(time.strptime(datestr, '%Y-%m-%d'))
    return date


def fetch_future_day(
        code,
        start=None,
        end=None,
        format='pandas',
        collections=DATABASE.future_day
):
    """
    :param code:
    :param start:
    :param end:
    :param format:
    :param collections:
    :return: pd.DataFrame
        columns = ["symbol", "dt", "open", "close", "high", "low", "vol"]
    """
    start = '1990-01-01' if start is None else str(start)[0:10]
    end = datetime.today().strftime('%Y-%m-%d') if end is None else str(end)[0:10]
    code = util_code_tolist(code, auto_fill=False)

    if util_date_valid(end) == True:

        _data = []
        cursor = collections.find(
            {
                'code': {
                    '$in': code
                },
                "date_stamp":
                    {
                        "$lte": util_date_stamp(end),
                        "$gte": util_date_stamp(start)
                    }
            },
            {"_id": 0},
            batch_size=10000
        )
        if format in ['dict', 'json']:
            return [data for data in cursor]
        for item in cursor:
            _data.append(
                [
                    str(item['code']),
                    float(item['open']),
                    float(item['high']),
                    float(item['low']),
                    float(item['close']),
                    float(item['position']),
                    float(item['price']),
                    float(item['trade']),
                    item['date']
                ]
            )

        # 多种数据格式
        if format in ['n', 'N', 'numpy']:
            _data = numpy.asarray(_data)
        elif format in ['list', 'l', 'L']:
            _data = _data
        elif format in ['P', 'p', 'pandas', 'pd']:
            _data = DataFrame(
                _data,
                columns=[
                    'code',
                    'open',
                    'high',
                    'low',
                    'close',
                    'position',
                    'price',
                    'trade',
                    'date'
                ]
            ).drop_duplicates()
            _data['date'] = pd.to_datetime(_data['date'])
            _data = _data.set_index('date', drop=False)
        else:
            logging.error(
                "Error fetch_future_day format parameter %s is none of  \"P, p, pandas, pd , n, N, numpy !\" "
                % format
            )
        return _data
    else:
        logging.warning('Something wrong with date')


def use_kline_analyze():
    print('=' * 100, '\n')
    print("KlineAnalyze 的使用方法：\n")
    kline = fetch_future_day('RBL8', start='2020-01-01')
    kline.columns = ['symbol', 'open', 'high', 'low', 'close', 'position', 'price', 'vol', 'dt']
    kline = kline.loc[:, ['symbol', 'dt', 'open', 'close', 'high', 'low', 'vol']]
    ka = KlineAnalyze(kline, name="本级别", bi_mode="new", max_xd_len=20, ma_params=(5, 34, 120), verbose=False)
    print("分型：", ka.fx_list, "\n")
    print("线段：", ka.xd_list, "\n")


if __name__ == '__main__':
    use_kline_analyze()
