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
财务指标结构

"""
from datetime import datetime

import pandas as pd

from czsc.Data.financial_mean import financial_dict


class FinancialStruct:

    def __init__(self, data):
        self.data = data
        self.ttm_data = self.get_ttm_data()
        self.factor = pd.DataFrame()
        # keys for CN, values for EN
        self.colunms_en = list(financial_dict.values())
        self.colunms_cn = list(financial_dict.keys())

    def __repr__(self):
        return '< Financial_Struct >'

    def __call__(self, *args, **kwargs):
        return self.data

    def get_report_by_date(self, code, date):
        return self.data.loc[pd.Timestamp(date), code]

    def get_key(self, code, reportdate, key):
        if isinstance(reportdate, list):
            return self.data.loc[(
                                     slice(
                                         pd.Timestamp(reportdate[0]),
                                         pd.Timestamp(reportdate[-1])
                                     ),
                                     code
                                 ),
                                 key]
        else:
            return self.data.loc[(pd.Timestamp(reportdate), code), key]

    def get_ttm_data(self):
        data = self.data
        ttm_data = pd.DataFrame()

        for index, item in data.iterrows():
            date = index[0]
            code = index[1]

            y = date.year
            m = date.month
            d = date.day

            if m == 12:
                x = item
            else:
                date1 = datetime(y - 1, m, d)
                date2 = datetime(y - 1, 12, 31)

                try:
                    x1 = data.loc[(date1, code)]
                    x2 = data.loc[(date2, code)]
                    x = item + x2 - x1
                except:
                    x = item

            ttm_data = ttm_data.append(x.to_frame(index).T)
            ttm_data.index.set_names(['report_date', 'code'], inplace=True)

        return ttm_data



    @property
    def ROIC(self):
        """
        ROIC＝NOPLAT(息前税后经营利润)÷IC(投入资本)
        NOPLAT＝EBIT207×(1－T)＝(营业利润＋财务费用－非经常性投资损益) ×(1－所得税率)
        IC＝有息负债＋净资产72－超额现金－非经营性资产
        (1－所得税率)=1-所得税93/净利润95
        有息负债=短期借款41+长期借款55+应付债券56+一年之内到期的非流动负债52}
        """
        if 'ROIC' not in self.factor.columns:
            df = self.ttm_data
            NOPLAT = df['EBIT'] * (1 - df['incomeTax'] / df['netProfit'])
            liability_with_interest = df['shortTermLoan'] + df['longTermLoans'] + df['bondsPayable'] \
                                      + df['noncurrentLiabilitiesDueWithinOneYear']
            IC = liability_with_interest + df['totalOwnersEquity']
            ROIC = NOPLAT / IC
            self.factor['ROIC'] = ROIC

        return self.factor['ROIC']

    @property
    def grossProfitMargin(self):
        """
        毛利率=毛利/营业收入×100%=（主营业务收入-主营业务成本）/主营业务收入×100%。
        """
        if 'rateOfReturnOnGrossProfitFromSales' not in self.factor.columns:
            df = self.ttm_data
            self.factor['grossProfitMargin'] = (df['operatingRevenue'] - df['operatingCosts']) / df['operatingRevenue']

        return self.factor['grossProfitMargin']

    @property
    def netProfitMargin(self):
        """
        净利润率=净利润/营业收入×100%
        """
        if 'netProfitMargin' not in self.factor.columns:
            df = self.ttm_data
            self.factor['netProfitMargin'] = df['netProfit'] / df['operatingRevenue']

        return self.factor['netProfitMargin']


if __name__ == '__main__':
    from czsc.Fetch.mongo import fetch_financial_report
    code = '000001'
    df = fetch_financial_report(code, start='2015-01-01')
    findata = FinancialStruct(df)
    # findata.data.to_csv("{} finance.csv".format(code))
    # findata.ttm_data
    print(findata.netProfitMargin)
