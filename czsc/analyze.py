# coding: utf-8
import numpy as np
import pandas as pd

from datetime import datetime

from czsc.Data.FinancialStruct import FinancialStruct
from czsc.Fetch.mongo import fetch_financial_report
from czsc.factors import threshold_dict
from czsc.Utils import util_log_info


def get_financial_scores():
    # 根据财务指标选择对公司打分
    today = datetime.today()
    year = today.year - 4
    start = datetime(year, today.month, today.day).strftime('%Y-%m-%d')
    total_reports_df = fetch_financial_report(start=start)
    code_list = total_reports_df.index.get_level_values(level=1).drop_duplicates()
    scores_df = pd.DataFrame(index=code_list, columns=['finance', 'holders'])
    # scores = pd.Series(index=code_list, dtype='float16', name='score')

    for code in code_list:
        try:
            df = total_reports_df.loc[(slice(None), code), :]
        except:
            continue

        util_log_info("Calculate {} financial scores!".format(code))

        findata = FinancialStruct(df)
        length = min(len(df), 12)
        factor = findata.financial_factor.iloc[:length].reset_index(level=1, drop=True)

        weight_list = list(range(length, 0, -1))
        weight = pd.Series(weight_list, name='weight') * 10 / sum(weight_list)
        weight.index = factor.index

        financial_score_df = pd.DataFrame(index=factor.index, columns=factor.columns)

        financial_score_df['ROIC'] = factor['ROIC'].apply(
            lambda x: 1 if x > threshold_dict['ROIC'] else 0 if x > 0 else -1)
        financial_score_df['grossProfitMargin'] = factor['grossProfitMargin'].apply(
            lambda x: 1 if x > threshold_dict['grossProfitMargin'] else 0 if x > 0 else -1)
        financial_score_df['netProfitMargin'] = factor['netProfitMargin'].apply(
            lambda x: 1 if x > threshold_dict['netProfitMargin'] else 0 if x > 0 else -1)
        financial_score_df['netProfitCashRatio'] = factor['netProfitCashRatio'].apply(
            lambda x: 1 if x > threshold_dict['netProfitCashRatio'] else 0 if x > 0 else -1)

        financial_score_df['operatingIncomeGrowth'] = factor['operatingIncomeGrowth'].apply(
            lambda x: 1 if x > threshold_dict['operatingIncomeGrowth'] else 0 if x > 0
            else -1 if x > -threshold_dict['operatingIncomeGrowth'] else -2)
        financial_score_df['continuedProfitGrowth'] = factor['continuedProfitGrowth'].apply(
            lambda x: 1 if x > threshold_dict['continuedProfitGrowth'] else 0 if x > 0
            else -1 if x > -threshold_dict['continuedProfitGrowth'] else -2)

        financial_score_df['assetsLiabilitiesRatio'] = factor['assetsLiabilitiesRatio'].apply(
            lambda x: 1 if x < threshold_dict['assetsLiabilitiesRatio'] else 0)
        financial_score_df['cashRatio'] = factor['cashRatio'].apply(
            lambda x: 1 if x > threshold_dict['cashRatio'] else 0)
        financial_score_df['inventoryRatio'] = factor['inventoryRatio'].apply(
            lambda x: 1 if x < threshold_dict['inventoryRatio'] else 0)
        # 小于0取正值会有问题，一般情况影响不大
        financial_score_df['interestCoverageRatio'] = factor['interestCoverageRatio'].apply(
            lambda x: 0 if (threshold_dict['interestCoverageRatio'][0] < x
                           < threshold_dict['interestCoverageRatio'][1]) else 1)

        scores_df.loc[code, 'finance'] = (financial_score_df.T * weight).sum().sum()

        columns = [
            'QFIISharesRatio', 'brokerSharesRatio', 'securitySharesRatio', 'fundsSharesRatio',
            'socialSecuritySharesRatio', 'privateEquitySharesRatio',
            'financialCompanySharesRatio', 'pensionInsuranceAgencySharesRatio'
        ]
        holder = findata.holders_factor.iloc[0][columns]
        scores_df.loc[code, 'holders'] = holder.sum()


    return scores_df


if __name__ == '__main__':
    scores = get_financial_scores()
    scores.to_csv('scores.csv')
