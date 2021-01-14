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

import pandas as pd


def resample_from_daily_data(day_data, type_='w'):
    """
    期货日线降采样
    """
    try:
        day_data = day_data.reset_index().set_index('date', drop=False)
    except:
        day_data = day_data.set_index('date', drop=False)

    CONVERSION = {
        'code': 'first',
        'exchange': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'settle': 'last',
        'volume': 'sum',
        'position': 'last',
        'date': 'last'
    } if 'position' in day_data.columns else {
        'code': 'first',
        'exchange': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'amount': 'sum',
        'date': 'last'
    }

    data = day_data.resample(type_, closed='right').apply(CONVERSION).dropna()
    return data.assign(date=pd.to_datetime(data.date)).set_index('date', drop=False)
