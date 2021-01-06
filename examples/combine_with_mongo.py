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
from czsc import KlineAnalyze
from czsc.Fetch.mongo import fetch_future_day
from czsc.Utils import ka_to_echarts, kline_pro
from pyecharts.charts import Tab
import webbrowser


def use_kline_analyze():
    print('=' * 100, '\n')
    print("KlineAnalyze 的使用方法：\n")
    # kline = fetch_future_day('RBL8', start='2020-01-01')
    kline = fetch_future_day('RBL8')
    # kline.columns = ['symbol', 'open', 'high', 'low', 'close', 'position', 'price', 'vol', 'dt']
    kline.rename(columns={'code': "symbol", "date": "dt", "trade": "vol"}, inplace=True)

    kline = kline.loc[:, ['symbol', 'dt', 'open', 'close', 'high', 'low', 'vol']]
    ka_day = KlineAnalyze(
        kline, name="本级别", bi_mode="new", max_count=2000, ma_params=(5, 34, 120), verbose=True, use_xd=True
    )

    width = "1300px"
    height = "650px"
    chart_day = ka_to_echarts(ka_day, width, height)
    # single = kline_pro(kline=ka_day.kline_raw)


    # tab = Tab()
    # tab.add(chart_day, "day")
    # tab.render('ka_day.html')

    chart_day_html = 'ka_day.html'
    chart_day.render(chart_day_html)

    webbrowser.open(chart_day_html)


if __name__ == '__main__':
    use_kline_analyze()
