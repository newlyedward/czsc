# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2020
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
from datetime import datetime

import numpy
import pandas as pd
import pymongo
from pandas import DataFrame

from czsc import KlineAnalyze
from czsc.ClUtils.ClTradeDate import util_get_real_date, trade_date_sse, util_date_valid, util_date_stamp

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
            return [code.upper()]

    elif isinstance(code, list):
        if auto_fill:
            return [util_code_tostr(item) for item in code]
        else:
            return [item.upper() for item in code]


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
        limit=2,
        format='pandas',
        collections=FACTOR_DATABASE.future_bi_day
):
    """
    :param code:
    :param start:
    :param end:
    :param limit: 如果有limit，直接按limit的数量取
    :param format:
    :param collections:
    :return: pd.DataFrame
        columns = ["code", "date", "value", "fx_mark"]
    """

    code = util_code_tolist(code, auto_fill=False)

    filter = {
        'code': {
            '$in': code
        }
    }

    projection = {"_id": 0}

    if start or end:
        start = '1990-01-01' if start is None else str(start)[0:10]
        end = datetime.today().strftime('%Y-%m-%d') if end is None else str(end)[0:10]

        if not util_date_valid(end):
            logging.warning('Something wrong with date')
            return

        filter.update(
            date_stamp={
                "$lte": util_date_stamp(end),
                "$gte": util_date_stamp(start)
            }
        )

        cursor = collections.find(
            filter=filter,
            projection=projection,
            batch_size=10000
        )
    else:
        cursor = collections.find(
            filter=filter,
            projection=projection,
            limit=limit,
            sort=[('date', -1)],
            batch_size=10000
        )

    _data = []

    if format in ['dict', 'json']:
        _data = [data for data in cursor]
        # 调整未顺序排列
        if not(start or end):
            _data = _data[::-1]
        return _data

    for item in cursor:
        _data.append(
            [
                str(item['code']),
                item['date'],
                str(item['fx_mark']),
                item['fx_start'],
                item['fx_end'],
                float(item['value'])
            ]
        )

    if not (start or end):
        _data = _data[::-1]

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
                'date',
                'fx_mark',
                'fx_start',
                'fx_end',
                'value'
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


def save_future_bi_day(code, collection=FACTOR_DATABASE.future_bi_day):
    try:
        logging.info(
            '##JOB12 Now Saving Future_BI_DAY==== {}'.format(str(code)),
        )
        code = code.upper()

        collection.create_index(
            [("code",
              pymongo.ASCENDING),
             ("date_stamp",
              pymongo.ASCENDING)]
        )
        # 首选查找数据库 是否 有 这个代码的数据
        filter = {'code': code}  # 只有通达信的指数和主连的code

        # 当前数据库已经包含了这个代码的数据， 继续增量更新
        if collection.count_documents(filter) > 2:

            # 接着上次获取的日期继续更新
            sort = [('date', -1)]
            limit = 2

            ref = client['factor']['future_bi_day'].find(
                filter=filter,
                sort=sort,
                limit=limit
            )

            start_date = ref[0]['date']
            logging.info(
                'UPDATE_Future_BI_DAY \n Trying updating {} from {} to {}'.format(code, start_date, datetime.today()),
            )
        # 当前数据库中没有这个代码的数据， 从1990-01-01 开始处理
        else:
            start_date = '1990-01-01'
            logging.info(
                'UPDATE_Future_BI_DAY \n Trying updating {} from {} to {}'.format(code, start_date, datetime.today()),
            )

        kline = fetch_future_day(code, start=start_date)

        kline.rename(columns={'code': "symbol", "date": "dt", "trade": "vol"}, inplace=True)

        ka_day = KlineAnalyze(
            kline, name="本级别", bi_mode="new", max_count=3000, ma_params=(5, 34, 120), verbose=False, use_xd=False
        )

        if len(ka_day.bi_list) < 3:
            logging.info(
                'UPDATE_Future_BI_DAY \n No ClData updated {} from {} to {}'.format(code, start_date, datetime.today())
            )
        # 最后一个数据未确定，需要删除
        collection.insert_many(
            ka_day.get_bi()[:-1]
        )
    except Exception as error:
        print(error)


if __name__ == '__main__':
    code = 'rbl8'
    # save_future_bi_day('rbl8')
    # df = fetch_future_bi_day('rbl8', start='2020-12-11', limit=4, format='p')
    fx = fetch_future_bi_day('rbl8', limit=1, format='dict')
    start = fx[0]['fx_end']
    bars = fetch_future_day(code, start=start, format='dict')
    print(bars)