# coding :utf-8
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
"""
因子展现
"""
import webbrowser
from datetime import datetime
from pyecharts.components import Table
from pyecharts.options import ComponentTitleOpts

import pandas as pd

from czsc.Data.FinancialStruct import FinancialStruct

presentation_dict = {
    'report_date': '报告日期',
    'grossProfitMargin': '毛利率',
    'netProfitMargin': '净利润率',
    'netProfitCashRatio': '经现/净利润',
    'operatingIncomeGrowth': '营收增长',
    'continuedProfitGrowth': '扣非增长',
    'assetsLiabilitiesRatio': '资产负债率',
    'interestCoverageRatio': '利息保障倍数',
    'cashRatio': '现金比率',
    'inventoryRatio': '存货比率',
}


def table_plot(data, columns=None, title=''):
    etable = Table()

    data.sort_index(ascending=False, inplace=True)
    data = data.applymap(lambda x: "{:.2f}%".format(x))

    if columns is None:
        rows = data.reset_index(level=0)
    else:
        rows = data[columns].reset_index(level=0)

    headers = [presentation_dict.get(field, field) for field in rows.columns.tolist()]
    rows['report_date'] = rows['report_date'].apply(lambda x: str(x)[0:10])
    rows = rows.values.tolist()

    etable.add(headers, rows)
    etable.set_global_opts(
        title_opts=ComponentTitleOpts(title=title)
    )

    return etable


if __name__ == '__main__':
    from czsc.Fetch.mongo import fetch_financial_report
    code = '300327'
    # df = fetch_financial_report(code, start='2015-01-01')
    df = fetch_financial_report(code)
    findata = FinancialStruct(df)
    table = table_plot(findata.factor, title=code)
    # findata.data.to_csv("{} finance.csv".format(code))
    # findata.ttm_data.to_csv("{} ttm finance.csv".format(code))
    table_path = '{}_factor.html'.format(code)
    table.render(table_path)
    webbrowser.open(table_path)
