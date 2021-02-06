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
    def __init__(self, bars, params, field='close'):
        self.bars = bars
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
    def __init__(self, bars=None, params=None):
        if params is None:
            params = [5, 10, 20, 30, 60]

        super().__init__(bars=bars, params=params)

        self.item_names = ['ma' + str(n) for n in self.params]

    def update(self):
        bar = self.bars[-1]
        length = len(self.bars)
        record = {'date': bar['date']}

        for n, item_name in zip(self.params, self.item_names):
            if length < n:
                continue

            if length == n:
                data = np.array([bar[self.field] for bar in self.bars])
                record[item_name] = data.mean()
                continue

            record[item_name] = self.value[-1][item_name] + (bar[self.field] - self.bars[-n - 1][self.field]) / n

        self.value.append(record)


class EMA(Indicator):
    def __init__(self, bars=None, params=None):
        if params is None:
            params = [5, 34]

        super().__init__(bars=bars, params=params)

        ema_func = {}
        for n in self.params:
            item_name = 'ema' + str(n)
            ema_func[item_name] = ema(n)

        self.ema_func = ema_func

    def update(self):
        bar = self.bars[-1]
        length = len(self.bars)
        record = {'date': bar['date']}

        for item_name in self.ema_func:

            if length < 2:
                record[item_name] = bar[self.field]
                continue

            record[item_name] = self.ema_func[item_name](bar[self.field], self.value[-1][item_name])

        self.value.append(record)


class BOLL(Indicator):
    def __init__(self, bars=None, params=None):
        if params is None:
            params = [20, 2]

        super().__init__(bars=bars, params=params)

        self.N = self.params[0]
        self.P = self.params[1]

        self.ma = MA(bars=self.bars, params=[self.N])

    def update(self):
        self.ma.update()

        bar = self.bars[-1]
        length = len(self.bars)
        record = {'date': bar['date']}

        if length < self.N:
            self.value.append(record)
            return

        date, record['boll'] = self.ma[-1].values()

        close = np.array([bar['close'] for bar in self.bars[-self.N:]])

        record['UB'] = record['boll'] + self.P * close.std(ddof=1)  # 使用估算标准差，ddof 自由度，分母为N-1
        record['LB'] = record['boll'] - self.P * close.std(ddof=1)

        self.value.append(record)


class MACD(Indicator):
    def __init__(self, bars=None, params=None):
        if params is None:
            params = [5, 34, 5]

        super().__init__(bars=bars, params=params)

        self.ema = EMA(bars=self.bars, params=self.params[:2])
        self.dea_func = ema(self.params[2])

    def update(self):
        """
        SHORT:=5;LONG:=34;MID:=5;
        DIF:EMA(CLOSE,SHORT)-EMA(CLOSE,LONG);
        DEA:EMA(DIF,MID);
        MACD:(DIF-DEA)*2;
        @param bars:
        @return:
        """
        # bar = bars[-1]
        # record = {'date': bar['date']}
        self.ema.update()
        date, short,  long = self.ema[-1].values()
        dif = short - long
        record = {'date': date, 'dif': dif}

        self.value.append(record)

        length = len(self.value)

        if length < 2:
            record['dea'] = dif
        else:
            record['dea'] = self.dea_func(record['dif'], self.value[-2]['dea'])

        record['macd'] = (record['dif'] - record['dea']) * 2


class IndicatorSet:
    def __init__(self, bars=None):
        if bars is None:
            self.bars = []
        else:
            self.bars = bars
        # self.ma = MA(self.bars)
        # self.ema = EMA(self.bars)
        # self.boll = BOLL(self.bars)
        self.macd = MACD(self.bars)

    def on_bar(self, bar):
        bar = bar.to_dict()
        self.bars.append(bar)
        self.update()

    def update(self):
        # self.ma.update()
        # self.ema.update()
        # self.boll.update()
        self.macd.update()


if __name__ == '__main__':
    indicators = IndicatorSet()
    code = 'apl8'
    freq = 'day'
    exchange = 'czce'
    bars = get_bar(code, freq=freq, exchange=exchange)
    bars.apply(indicators.on_bar, axis=1)
