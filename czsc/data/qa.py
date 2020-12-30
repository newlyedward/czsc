# coding: utf-8
import logging
import time
from datetime import datetime

import numpy
import pandas as pd
import pymongo
from pandas import DataFrame

from czsc.data.trade_date import util_get_real_date, trade_date_sse, util_date_valid, util_date_stamp, util_get_next_day
from czsc.data.transform import util_to_json_from_pandas

uri = 'mongodb://localhost:27017/factor'
client = pymongo.MongoClient(uri)
QA_DATABASE = client.quantaxis
FACTOR_DATABASE = client.factor


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


def now_time():
    return str(util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
           ' 17:00:00' if datetime.datetime.now().hour < 15 else str(util_get_real_date(
        str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'


def fetch_future_day(
        code,
        start=None,
        end=None,
        format='pandas',
        collections=QA_DATABASE.future_day
):
    """
    :param code:
    :param start:
    :param end:
    :param format:
    :param collections:
    :return: pd.DataFrame
        columns = ["code", "date", "open", "close", "high", "low", "position", "price", "trade"]
    """
    start = '1990-01-01' if start is None else str(start)[0:10]
    end = datetime.today().strftime('%Y-%m-%d') if end is None else str(end)[0:10]
    code = util_code_tolist(code, auto_fill=False)

    if util_date_valid(end):

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


def fetch_future_bi_day(
        code,
        start=None,
        end=None,
        format='pandas',
        collections=FACTOR_DATABASE.future_bi_day
):
    """
    :param code:
    :param start:
    :param end:
    :param format:
    :param collections:
    :return: pd.DataFrame
        columns = ["code", "date", "value", "fx_mark"]
    """
    start = '1990-01-01' if start is None else str(start)[0:10]
    end = datetime.today().strftime('%Y-%m-%d') if end is None else str(end)[0:10]
    code = util_code_tolist(code, auto_fill=False)

    if util_date_valid(end):

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
                    str(item['fx_mark']),
                    float(item['value']),
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
                    'fx_mark',
                    'value',
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


def save_future_bi_day(code, collection=FACTOR_DATABASE.future_bi_day):
    try:
        logging.info(
            '##JOB12 Now Saving Future_BI_DAY==== {}'.format(str(code)),
        )

        # 首选查找数据库 是否 有 这个代码的数据
        ref = collection.find({'code': str(code)[0:4]})
        end_date = str(now_time())[0:10]

        # 当前数据库已经包含了这个代码的数据， 继续增量更新
        # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
        if ref.count() > 0:

            # 接着上次获取的日期继续更新
            start_date = ref[ref.count() - 1]['date']

            logging.info(
                'UPDATE_Future_BI_DAY \n Trying updating {} from {} to {}'.format(code, start_date, end_date),
            )
            if start_date != end_date:
                collection.insert_many(
                    util_to_json_from_pandas(
                        fetch_future_day(
                            str(code),
                            util_get_next_day(start_date),
                            end_date
                        )
                    )
                )

        # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
        else:
            start_date = '2001-01-01'
            logging.info(
                'UPDATE_Future_DAY \n Trying updating {} from {} to {}'.format(code, start_date, end_date),
            )
            if start_date != end_date:
                collection.insert_many(
                    util_to_json_from_pandas(
                        fetch_future_day(
                            str(code),
                            start_date,
                            end_date
                        )
                    )
                )
    except Exception as error:
        print(error)
