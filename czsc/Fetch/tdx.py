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
import os
import re
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pytdx.reader import TdxDailyBarReader, TdxExHqDailyBarReader, TdxMinBarReader, TdxLCMinBarReader

from czsc.Setting import TDX_DIR
from czsc.Utils import util_log_info

_SH_DIR = '{}{}{}'.format(TDX_DIR, os.sep, 'vipdoc\\sh')
_SZ_DIR = '{}{}{}'.format(TDX_DIR, os.sep, 'vipdoc\\sz')
_DS_DIR = '{}{}{}'.format(TDX_DIR, os.sep, 'vipdoc\\ds')


def _get_sh_sz_list():
    """
    读取上海深圳交易所行情目录的文件列表，并对市场，品种和代码分类
    sh000015.day   期货    ('28', 'AP2003')
    'sse'     # 上海证券交易所       sh  6位数字代码
               前两位 "60"    A股
                     "90"    B股
                     "00", "88", "99" 指数
                     "50", "51"       基金
                     "01", "10", "11", "12", "13", "14" 债券，和深圳有重合

    'szse'    # 深圳证券交易所       sz  6位数字代码
            前两位 "00", "30"  A股
                  "20"
                  "39"       指数
                  "15", "16" 基金
                  "10", "11", "12", "13", "14" 债券，和深圳有重合
    pattern = "^(?P<tdx_code>[shz]{2})#(?P<code>\d{6})\.day"
    """
    sh_dir = '{}{}{}'.format(_SH_DIR, os.sep, 'lday')
    sh_list = os.listdir(sh_dir)

    pattern = "^(?P<tdx_code>sh)(?P<code>\d{6})\.day"
    data = [re.match(pattern, x) for x in sh_list]
    try:
        sh_df = pd.DataFrame([x.groupdict() for x in data])
    except:
        util_log_info("{} can't be analyzed by pattern ({}) }".format(_SH_DIR, pattern))
        return None

    sh_df['exchange'] = 'sse'

    SH_CODE_HEAD_TO_TYPE = {
        '60': 'stock',
        '68': 'stock',  # 科创板
        '90': 'stock',  # B股
        '00': 'index',
        '88': 'index',
        '99': 'index',
        '50': 'fund',
        '51': 'fund',
        '58': 'ETF',   # 科创板ETF
        '20': 'bond',  # 国债逆回购
        '01': 'bond',  # 贴债
        '02': 'bond',
        '10': 'bond',
        '11': 'bond',
        '12': 'bond',
        '13': 'bond',
        '14': 'bond',
        '75': 'bond',
    }

    sh_df['instrument'] = sh_df.code.apply(lambda x: SH_CODE_HEAD_TO_TYPE[x[:2]])

    sz_dir = '{}{}{}'.format(_SZ_DIR, os.sep, 'lday')
    sz_list = os.listdir(sz_dir)

    pattern = "^(?P<tdx_code>sz)(?P<code>\d{6})\.day"
    data = [re.match(pattern, x) for x in sz_list]
    try:
        sz_df = pd.DataFrame([x.groupdict() for x in data])
    except:
        util_log_info("{} can't be analyzed by pattern ({}) }".format(_SZ_DIR, pattern))
        return None

    sz_df['exchange'] = 'szse'

    SZ_CODE_HEAD_TO_TYPE = {
        '00': 'stock',  # 中小板 主板
        '30': 'stock',  # 创业板
        '20': 'stock',  # B股
        '39': 'index',
        '15': 'fund',
        '16': 'fund',
        '10': 'bond',
        '11': 'bond',
        '12': 'bond',
        '13': 'bond',  # 逆回购
        '14': 'bond',  # 贴息国债，就一个品种
        '38': 'bond',  # 贴息国债，就一个品种
        '18': 'reits',  # REITS
        '08': 'unknown',
    }
    sz_df['instrument'] = sz_df.code.apply(lambda x: SZ_CODE_HEAD_TO_TYPE[x[:2]])

    return pd.concat([sh_df, sz_df])


def _get_ds_list():
    """
    读取扩展行情目录的文件列表，并对市场，品种和代码分类
    47#TS2009.day   期货    ('28', 'AP2003')
    7#IO760795.day  期权    ('7', 'IO760795')
    5#V 7C0D49.day  期权 中间有空格，特殊处理
    102#980001.day  102 国证指数
    pattern = "^(?P<tdx_code>\d{1,3})#(?P<code>.+)\.day"
    """
    DS_CODE_TO_TYPE = {
        '4': {'exchange': 'czce', 'instrument': 'option'},
        '5': {'exchange': 'dce', 'instrument': 'option'},
        '6': {'exchange': 'shfe', 'instrument': 'option'},
        '7': {'exchange': 'cffex', 'instrument': 'option'},
        '8': {'exchange': 'sse', 'instrument': 'option'},
        '9': {'exchange': 'szse', 'instrument': 'option'},
        '28': {'exchange': 'czce', 'instrument': 'future'},
        '29': {'exchange': 'dce', 'instrument': 'future'},
        '30': {'exchange': 'shfe', 'instrument': 'future'},
        '47': {'exchange': 'cffex', 'instrument': 'future'},
        '102': {'exchange': 'sse szse', 'instrument': 'index'},
    }
    ds_dir = '{}{}{}'.format(_DS_DIR, os.sep, 'lday')
    ds_list = os.listdir(ds_dir)

    pattern = "^(?P<tdx_code>\d{1,3})#(?P<code>.+)\.day"
    data = [re.match(pattern, x) for x in ds_list]
    try:    # 注释条码用来显示pattern不能识别的文件名
        # for i, x in enumerate(data):
        #     if not x:
        #         util_log_info('{}'.format(ds_list[i]))
        ds_df = pd.DataFrame([x.groupdict() for x in data])
    except:
        util_log_info("{} can't be analyzed by pattern ({}) }".format(_DS_DIR, pattern))
        return None

    ds_df['exchange'] = ds_df.tdx_code.apply(lambda x: DS_CODE_TO_TYPE[x]['exchange'] if x in DS_CODE_TO_TYPE else None)
    ds_df['instrument'] = ds_df.tdx_code.apply(
        lambda x: DS_CODE_TO_TYPE[x]['instrument'] if x in DS_CODE_TO_TYPE else None)

    return ds_df


def get_security_list():
    securities: DataFrame = pd.concat([_get_sh_sz_list(), _get_ds_list()])
    return securities.set_index('code')


SECURITY_DATAFRAME = get_security_list()


def _get_tdx_code_from_security_dataframe(code, exchange):
    try:
        recorder = SECURITY_DATAFRAME.loc[code]
    except:
        util_log_info("Can't get tdx_code from {}".format(code))
        return

    if isinstance(recorder, pd.Series):
        return recorder['tdx_code']

    try:
        return recorder.loc[recorder['exchange'] == exchange].loc[code, 'tdx_code']
    except:
        util_log_info('Not only one {} in the list , please provide exchange or instrument'.format(code))
        return recorder.tdx_code[0]


def _generate_path(code, freq, tdx_code):
    # code = code.upper()
    # freq = freq.lower()

    ext = {
        'day': '.day',
        '5min': '.lc5',
        '1min': '.lc1',
    }

    dir = {
        'day': 'lday',
        '5min': 'fzline',
        '1min': '.minline',
    }

    try:
        if tdx_code == 'sz':
            dir_name = '{}{}{}'.format(_SZ_DIR, os.sep, dir[freq])
            filename = tdx_code + code + ext[freq]
        elif tdx_code == 'sh':
            dir_name = '{}{}{}'.format(_SH_DIR, os.sep, dir[freq])
            filename = tdx_code + code + ext[freq]
        else:
            dir_name = '{}{}{}'.format(_DS_DIR, os.sep, dir[freq])
            filename = tdx_code + '#' + code + ext[freq]
    except KeyError:
        util_log_info('Not supported Frequency {}!'.format(freq))
        return

    file_path = os.path.join(dir_name, filename)
    return file_path


def get_bar(code, start=None, end=None, freq='day', exchange=None):
    """
    股票成交量 volume 单位是100股
    """
    code = code.upper()
    freq = freq.lower()

    try:
        tdx_code = _get_tdx_code_from_security_dataframe(code, exchange)
    except:
        util_log_info("Can't get tdx_code from {}".format(code))
        return

    file_path = _generate_path(code, freq, tdx_code)

    if not file_path:
        return

    if tdx_code in ['sh', 'sz']:
        if freq in ['day']:
            reader = TdxDailyBarReader()
            df = reader.get_df(file_path)
        elif freq in ['1min', '5min']:
            reader = TdxLCMinBarReader()
            df = reader.get_df(file_path)
        else:
            util_log_info('Not supported frequency {}'.format(freq))
            return
    else:
        if freq in ['day']:
            reader = TdxExHqDailyBarReader()
            df = reader.get_df(file_path)
        elif freq in ['1min', '5min']:
            reader = TdxLCMinBarReader()
            df = reader.get_df(file_path)
        else:
            util_log_info('Not supported frequency {}'.format(freq))
            return

    if len(df) < 1:
        return

    recorder = SECURITY_DATAFRAME.loc[code]

    if isinstance(recorder, pd.DataFrame):
        instrument = recorder.loc[recorder['tdx_code'] == tdx_code].loc[code, 'instrument']
        exchange = recorder.loc[recorder['tdx_code'] == tdx_code].loc[code, 'exchange']
    else:
        instrument = recorder['instrument']
        exchange = recorder['exchange']

    if instrument in ['future', 'option']:
        df.rename(columns={'amount': "position", "jiesuan": "settle"})

    if start:
        start = pd.to_datetime(start)
        df = df[df.index >= start]

    if end:
        end = pd.to_datetime(end)
        df = df[df.index <= end]

    df['date'] = df.index
    df = df.assign(code=code, exchange=exchange)

    return df


if __name__ == "__main__":
    # ds_df = _get_ds_list()
    # sz_sh_df = _get_sh_sz_list()
    # security_df = get_security_list()
    # hq = fetch_future_day('rbl8')
    hq = get_bar('600633')
