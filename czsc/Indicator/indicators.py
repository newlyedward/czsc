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

from abc import ABCMeta
import pandas as pd
import numpy as np

from czsc.Fetch.tdx import get_bar
from czsc.Indicator import ema


class Indicator(metaclass=ABCMeta):
    def __init__(self, params, field='close'):
        self.value = []
        self.field = field
        self.params = params

    def __call__(self, *args, **kwargs):
        return self.value

    def __getitem__(self, item):
        return self.value[item]

    def update(self, bars):
        raise NotImplementedError


class MA(Indicator):
    def __init__(self, params=None):
        if params is None:
            params = [5, 10, 20, 30, 60]

        super().__init__(params=params)

    def update(self, bars):
        bar = bars[-1]
        length = len(bars)
        record = {'date': bar['date']}

        for n in self.params:
            if length < n:
                continue

            item_name = 'ma' + str(n)
            if length == n:
                data = np.array([bar[self.field] for bar in bars])
                record[item_name] = data.mean()
                continue

            record[item_name] = self.value[-1][item_name] + (bar[self.field] - bars[-n - 1][self.field]) / n

        self.value.append(record)


class EMA(Indicator):
    def __init__(self, params=None):
        if params is None:
            params = [5, 34]

        super().__init__(params=params)

        ema_func = {}
        for n in self.params:
            item_name = 'ma' + str(n)
            ema_func[item_name] = ema(n)

        self.ema_func = ema_func

    def update(self, bars):
        bar = bars[-1]
        length = len(bars)
        record = {'date': bar['date']}

        for n in self.params:
            item_name = 'ma' + str(n)

            if length < 2:
                record[item_name] = bar[self.field]
                continue

            record[item_name] = self.ema_func[item_name](bar[self.field], self.value[-1][item_name])

        self.value.append(record)


class Boll(Indicator):
    def __init__(self, params=None):
        if params is None:
            params = [20, 2]

        super().__init__(params=params)

        self.N = self.params[0]
        self.P = self.params[1]

        self.ma = MA(params=[self.N])

    def update(self, bars):
        self.ma.update(bars)

        bar = bars[-1]
        length = len(bars)
        record = {'date': bar['date']}

        if length < self.N:
            return

        item_name = 'ma' + str(self.N)
        record['boll'] = self.ma[-1][item_name]

        close = np.array([bar['close'] for bar in bars[-self.N:]])

        record['UB'] = record['boll'] + self.P * close.std(ddof=1)   # 使用估算标准差，ddof 自由度，分母为N-1
        record['LB'] = record['boll'] - self.P * close.std(ddof=1)

        self.value.append(record)


class IndicatorSet:
    def __init__(self):
        self.bars = []
        # self.ma = MA()
        # self.ema = EMA()
        self.boll = Boll()

    def on_bar(self, bar):
        bar = bar.to_dict()
        self.bars.append(bar)
        self.update()

    def update(self):
        # self.ma.update(self.bars)
        # self.ema.update(self.bars)
        self.boll.update(self.bars)


def BOLL(bars, N=20, P=2):
    """
    布林线
    """
    length = min(len(bars), N)

    if length > N:
        close = np.array([bar['close'] for bar in bars[-N:]])
    else:
        close = np.array([bar['close'] for bar in bars[-N:]])

    boll = close.mean()
    UB = boll + P * close.std()
    LB = boll - P * close.std()
    return {'BOLL': boll, 'UB': UB, 'LB': LB}


if __name__ == '__main__':
    indicators = IndicatorSet()
    code = 'apl8'
    freq = 'day'
    exchange = 'czce'
    bars = get_bar(code, freq=freq, exchange=exchange)
    bars.apply(indicators.on_bar, axis=1)
