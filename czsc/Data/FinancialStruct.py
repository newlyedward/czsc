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
import numpy as np

from czsc.Data.financial_mean import financial_dict


class FinancialStruct:

    def __init__(self, data):
        self.data = data
        self.ttm_data = self.get_ttm_data()
        self.financial_factor = pd.DataFrame()
        self.holders_factor = pd.DataFrame()
        # keys for CN, values for EN
        self.colunms_en = list(financial_dict.values())
        self.colunms_cn = list(financial_dict.keys())
        self.init_factor()

    def __repr__(self):
        return '< Financial_Struct >'

    def __call__(self, *args, **kwargs):
        return self.data

    def init_factor(self):
        # factors = ['ROIC']
        financial_columns = [
            'ROIC', 'grossProfitMargin', 'netProfitMargin', 'netProfitCashRatio',
            'operatingIncomeGrowth', 'continuedProfitGrowth',
            'assetsLiabilitiesRatio', 'interestCoverageRatio', 'cashRatio', 'inventoryRatio'
        ]

        for field in financial_columns:
            eval('self.{}'.format(field))

        self.holders_factor['avgSharesRatio'] = 1 / self.data['numberOfShareholders'] * 1000
        self.holders_factor['institutionSharesRatio'] = \
            self.data['institutionShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgInstitutionSharesRatio'] = \
            self.holders_factor['institutionSharesRatio'] / self.data['institutionNumber']

        self.holders_factor['QFIISharesRatio'] = \
            self.data['QFIIShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgQFIISharesRatio'] = \
            self.holders_factor['QFIISharesRatio'] / self.data['QFIIInstitutionNumber']

        self.holders_factor['brokerSharesRatio'] = \
            self.data['brokerShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgBrokerSharesRatio'] = \
            self.holders_factor['brokerSharesRatio'] / self.data['brokerNumber']

        self.holders_factor['securitySharesRatio'] = \
            self.data['securityShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgSecuritySharesRatio'] = \
            self.holders_factor['securitySharesRatio'] / self.data['securityNumber']

        self.holders_factor['fundsSharesRatio'] = \
            self.data['fundsShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgFundsSharesRatio'] = \
            self.holders_factor['fundsSharesRatio'] / self.data['fundsNumber']

        self.holders_factor['socialSecuritySharesRatio'] = \
            self.data['socialSecurityShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgSocialSecuritySharesRatio'] = \
            self.holders_factor['socialSecuritySharesRatio'] / self.data['socialSecurityNumber']

        self.holders_factor['privateEquitySharesRatio'] = \
            self.data['privateEquityShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgPrivateEquitySharesRatio'] = \
            self.holders_factor['privateEquitySharesRatio'] / self.data['privateEquityNumber']

        self.holders_factor['financialCompanySharesRatio'] = \
            self.data['financialCompanyShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgFinancialCompanySharesRatio'] = \
            self.holders_factor['financialCompanySharesRatio'] / self.data['financialCompanyNumber']

        self.holders_factor['pensionInsuranceAgencySharesRatio'] = \
            self.data['pensionInsuranceAgencyShareholding'] / self.data['listedAShares'] * 1000
        self.holders_factor['avgPensionInsuranceAgencySharesRatio'] = \
            self.holders_factor['pensionInsuranceAgencySharesRatio'] / self.data['pensionInsuranceAgencyNumber']

        self.holders_factor.fillna(0, inplace=True)
        self.holders_factor.replace(np.inf, 0, inplace=True)

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
        if 'ROIC' not in self.financial_factor.columns:
            df = self.ttm_data
            NOPLAT = df['EBIT'] * (1 - df['incomeTax'] / df['netProfit'])
            liability_with_interest = df['shortTermLoan'] + df['longTermLoans'] + df['bondsPayable'] \
                                      + df['noncurrentLiabilitiesDueWithinOneYear']
            IC = liability_with_interest + df['totalOwnersEquity']
            ROIC = NOPLAT / IC
            self.financial_factor['ROIC'] = ROIC * 100

        return self.financial_factor['ROIC']

    @property
    def grossProfitMargin(self):
        """
        毛利率=毛利/营业收入×100%=（主营业务收入-主营业务成本）/主营业务收入×100%。
        """
        if 'grossProfitMargin' not in self.financial_factor.columns:
            df = self.data
            # df = self.ttm_data
            # self.financial_factor['grossProfitMargin'] = (df['operatingRevenue'] - df['operatingCosts']) \
            #                                    / df['operatingRevenue'] * 100
            self.financial_factor['grossProfitMargin'] = df['rateOfReturnOnGrossProfitFromSales']

        return self.financial_factor['grossProfitMargin']

    @property
    def netProfitMargin(self):
        """
        净利润率=净利润/营业收入×100%
        """
        if 'netProfitMargin' not in self.financial_factor.columns:
            # df = self.ttm_data
            # self.financial_factor['netProfitMargin'] = df['netProfit'] / df['operatingRevenue'] * 100
            df = self.data
            self.financial_factor['netProfitMargin'] = df['rateOfReturnOnNetSalesProfit']
        return self.financial_factor['netProfitMargin']

    @property
    def netProfitCashRatio(self):
        """
        直接使用TDX数据
        净利润现金比率=经营现金流量净额 /净利润
        """
        if 'netProfitCashRatio' not in self.financial_factor.columns:
            df = self.data
            self.financial_factor['netProfitCashRatio'] = df['cashFlowRateAndNetProfitRatioOfOperatingActivities']

        return np.abs(self.financial_factor['netProfitCashRatio']) * np.sign(df['netCashFlowsFromOperatingActivities'])

    @property
    def operatingIncomeGrowth(self):
        """
        直接使用TDX数据
        营收增长
        """
        if 'operatingIncomeGrowth' not in self.financial_factor.columns:
            df = self.data
            # df = self.ttm_data
            # # ttm的同比数据，平滑季节性因素
            # self.financial_factor['operatingIncomeGrowth'] = df['operatingRevenue'] / df['operatingRevenue'].shift(4) * 100 - 100
            self.financial_factor['operatingIncomeGrowth'] = df['operatingIncomeGrowth']

        return self.financial_factor['operatingIncomeGrowth']

    @property
    def continuedProfitGrowth(self):
        """
        扣非数据计算复杂，直接使用TDX数据
        扣非净利润=净利润 - 非经常性损益
        非经常性损益 = 投资收益、公允价值变动损益、以及营业外收入和支出。

        """
        if 'continuedProfitGrowth' not in self.financial_factor.columns:
            df = self.data
            self.financial_factor['continuedProfitGrowth'] = df['continuedProfitGrowthRate']

        return self.financial_factor['continuedProfitGrowth']

    @property
    def assetsLiabilitiesRatio(self):
        """
        直接使用TDX数据
        资产负债率
        """
        if 'assetsLiabilitiesRatio' not in self.financial_factor.columns:
            df = self.data
            self.financial_factor['assetsLiabilitiesRatio'] = df['assetsLiabilitiesRatio']

        return self.financial_factor['assetsLiabilitiesRatio']

    @property
    def inventoryRatio(self):
        """
        直接使用TDX数据
        存货比率 = 库存/流动资产
        """
        if 'inventoryRatio' not in self.financial_factor.columns:
            df = self.data
            # ttm的同比数据，平滑季节性因素
            self.financial_factor['inventoryRatio'] = df['inventoryRatio']

        return self.financial_factor['inventoryRatio']

    @property
    def interestCoverageRatio(self):
        """
        直接使用TDX数据
        利息保障倍数 = (利润总额+财务费用（仅指利息费用部份）)/利息费用
        利息保障倍数=EBIT/利息费用
        分母：“利息费用”：我国的会计实务中将利息费用计入财务费用,并不单独记录，所以作为外部使用者通常得不到准确的利息费用的数据，
        分析人员通常用财务费用代替利息费用进行计算，所以存在误差。
        """
        if 'interestCoverageRatio' not in self.financial_factor.columns:
            df = self.data
            self.financial_factor['interestCoverageRatio'] = df['interestCoverageRatio']

        return self.financial_factor['interestCoverageRatio']

    @property
    def cashRatio(self):
        """
        直接使用TDX数据
        现金比率 = (货币资金+有价证券)÷流动负债
        """
        if 'cashRatio' not in self.financial_factor.columns:
            df = self.data
            self.financial_factor['cashRatio'] = df['cashRatio']

        return self.financial_factor['cashRatio']


if __name__ == '__main__':
    from czsc.Fetch.mongo import fetch_financial_report

    code = '601628'
    # df = fetch_financial_report(code, start='2015-01-01')
    df = fetch_financial_report(code, start='2017-03-01')
    findata = FinancialStruct(df)
    findata.holders_factor.to_csv("{} holders_factor.csv".format(code))
    # findata.holders_factor.to_csv("holders_factor.csv")
    # findata.ttm_data.to_csv("{} ttm finance.csv".format(code))
    print(findata.operatingIncomeGrowth)
