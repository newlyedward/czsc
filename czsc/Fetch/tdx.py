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

# 从TDX磁盘空间读取数据
import datetime
import os
import re
import pandas as pd
import numpy as np

from czsc.Setting import TDX_DIR
from czsc.Utils import util_log_info

DS_DIR = '{}{}{}'.format(TDX_DIR, os.sep, 'vipdoc\\ds')


def fetch_future_list(market='all'):
    """
    47#TS2009.day   期货    ('28', 'AP2003')
    7#IO760795.day  期权    ('7', 'IO760795')
    5#V 7C0D49.day  期权 中间有空格，特殊处理
    pattern = "^(?P<market>\d{1,2})#(?P<code>.+)\.day"
    market: all 包含所有期货和期权
            future 只返回期货
            option 只返回期权
            'cffex',  # 中金所             47
            'czce',   # 郑州商品交易所       28
            'dce',    # 大连商品交易所       29
            'shfe',   # 上海期货交易所       30
    """
    lday_dir = '{}{}{}'.format(DS_DIR, os.sep, 'lday')
    ds_list = os.listdir(lday_dir)
    market = market.upper()

    pattern = "^(?P<market>\d{1,2})#(?P<code>.+)\.day"
    data = [re.match(pattern, x) for x in ds_list]
    try:
        future_list = pd.DataFrame([x.groupdict() for x in data])
    except:
        util_log_info("{} can't be analyzed by pattern ({}) }".format(ds_dir, pattern))
        return None

    # todo 根据 市场和品种 代码返回 list
    if market == 'ALL':
        return future_list


def get_future_market_from_code(code):
    """
    返回 TDX 的市场代码
    """
    code = code.upper()
    future_list = fetch_future_list()
    # todo 以后改为返回字符格式或者代码格式两种方式
    return future_list[future_list['code'] == code].iloc[0, 0]


def fetch_future_day(code, start=None, end=None):
    """

    """
    lday_dir = '{}{}{}'.format(DS_DIR, os.sep, 'lday')
    market = get_future_market_from_code(code)
    filename = market + '#' + code.upper() + '.day'
    file_path = os.path.join(lday_dir, filename)

    if not os.path.exists(file_path):
        util_log_info('{} hq is not exists!'.format(code))
        return None

    f = open(file_path, "rb")

    def _tdx_future_day_hq(file_handler):
        # 和QA的字段名
        names = 'date', 'open', 'high', 'low', 'close', 'position', 'trade', 'comment'
        offsets = tuple(range(0, 31, 4))
        formats = 'i4', 'f4', 'f4', 'f4', 'f4', 'i4', 'i4', 'i4'

        dt_types = np.dtype({'names': names, 'offsets': offsets, 'formats': formats}, align=True)
        hq = pd.DataFrame(np.fromfile(file_handler, dt_types))
        hq['date'] = hq['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
        hq.set_index('date', inplace=True, drop=False)
        hq = hq.assign(code=code)
        return hq

    start = pd.to_datetime('1990-01-01', format='%Y-%m-%d') if start is None else pd.to_datetime(start)
    end = datetime.date.today().strftime('%Y-%m-%d') if end is None else pd.to_datetime(end)

    data = _tdx_future_day_hq(f)

    if start is not None:
        start = pd.to_datetime(start)
        data = data[start <= data.index]

    if end is not None:
        end = pd.to_datetime(end)
        data = data[data.index <= end]

    return data


if __name__ == "__main__":
    df = fetch_future_list()
    hq = fetch_future_day('rbl8')



