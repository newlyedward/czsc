# coding: utf-8
import numpy as np
import pandas as pd

from datetime import datetime

from czsc.Data.FinancialStruct import FinancialStruct
from czsc.Fetch.mongo import fetch_financial_report
from czsc.factors import threshold_dict


def get_financial_scores():
    # 根据财务指标选择对公司打分
    today = datetime.today()
    year = today.year - 4
    start = datetime(year, today.month, today.day).strftime('%Y-%m-%d')
    total_reports_df = fetch_financial_report(start=start)
    code_list = total_reports_df.index.get_level_values(level=1).drop_duplicates()
    scores = pd.Series(index=code_list, dtype='float16')

    for code in code_list:
        try:
            df = total_reports_df.loc[(slice(None), code), :]
        except:
            continue

        findata = FinancialStruct(df)
        length = min(len(df), 12)
        factor = findata.factor.iloc[:length].reset_index(level=1, drop=True)

        weight_list = list(range(length, 0, -1))
        weight = pd.Series(weight_list, name='weight') * 10 / sum(weight_list)
        weight.index = factor.index

        score_df = pd.DataFrame(index=factor.index, columns=factor.columns)
        score_df['ROIC'] = np.sign(factor['ROIC'] - threshold_dict['ROIC'])
        score_df['grossProfitMargin'] = np.sign(factor['grossProfitMargin'] - threshold_dict['grossProfitMargin'])
        score_df['netProfitMargin'] = np.sign(factor['netProfitMargin'] - threshold_dict['netProfitMargin'])
        score_df['netProfitCashRatio'] = np.sign(factor['netProfitCashRatio'] - threshold_dict['netProfitCashRatio'])
        score_df['operatingIncomeGrowth'] \
            = np.sign(factor['operatingIncomeGrowth'] - threshold_dict['operatingIncomeGrowth'])
        score_df['continuedProfitGrowth'] \
            = np.sign(factor['continuedProfitGrowth'] - threshold_dict['continuedProfitGrowth'])

        score_df['assetsLiabilitiesRatio'] = score_df['assetsLiabilitiesRatio'].apply(
            lambda x: 1 if x < threshold_dict['assetsLiabilitiesRatio'] else 0)
        score_df['cashRatio'] = score_df['cashRatio'].apply(
            lambda x: 1 if x > threshold_dict['cashRatio'] else 0)
        score_df['inventoryRatio'] = score_df['inventoryRatio'].apply(
            lambda x: 1 if x < threshold_dict['inventoryRatio'] else 0)
        score_df['interestCoverageRatio'] = score_df['interestCoverageRatio'].apply(
            lambda x: 0 if threshold_dict['interestCoverageRatio'][0] < x \
                           < threshold_dict['interestCoverageRatio'][1] else 1)

        scores[code] = (score_df.T * weight).sum().sum()

    return scores


if __name__ == '__main__':
    scores = get_financial_scores()
