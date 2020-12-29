# coding: utf-8
from czsc import KlineAnalyze
from czsc.data.qa import fetch_future_day
from czsc.utils import ka_to_echarts
from pyecharts.charts import Tab
import webbrowser


def use_kline_analyze():
    print('=' * 100, '\n')
    print("KlineAnalyze 的使用方法：\n")
    # kline = fetch_future_day('RBL8', start='2020-01-01')
    kline = fetch_future_day('RBL8')
    kline.columns = ['symbol', 'open', 'high', 'low', 'close', 'position', 'price', 'vol', 'dt']
    kline.rename(columns={'code': "symbol", "date": "dt", "trade": "vol"}, inplace=True)

    kline = kline.loc[:, ['symbol', 'dt', 'open', 'close', 'high', 'low', 'vol']]
    ka_day = KlineAnalyze(
        kline, name="本级别", bi_mode="new", max_count=2000, ma_params=(5, 34, 120), verbose=True, use_xd=True
    )

    width = "2500px"
    height = "850px"
    chart_day = ka_to_echarts(ka_day, width, height)

    tab = Tab()
    tab.add(chart_day, "day")
    chart_day_html = 'ka_day.html'
    tab.render('ka_day.html')

    webbrowser.open('ka_day.html')


if __name__ == '__main__':
    use_kline_analyze()
