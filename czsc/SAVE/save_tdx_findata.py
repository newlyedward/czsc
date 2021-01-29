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
import os
import re

import pandas as pd
import pymongo

from pytdx.reader.history_financial_reader import HistoryFinancialReader

from czsc.SAVE import ASCENDING
from czsc.Setting import CLIENT
from czsc.Setting import TDX_DIR
from czsc.Utils import util_log_info
from czsc.Utils.transformer import util_to_json_from_pandas

QA_DATABASE = CLIENT.quantaxis
_CW_DIR = '{}{}{}'.format(TDX_DIR, os.sep, 'vipdoc\\cw')


def save_financial_files():
    """
    将tdx目录下的gpcw财务数据存储到mongo数据库
    """
    coll = QA_DATABASE.financial
    coll.create_index(
        [("code", ASCENDING), ("report_date", ASCENDING)], unique=True)

    pattern = "^(gpcw)(?P<date>\d{8})\.dat"    # gpcw20210930.dat
    for filename in os.listdir(_CW_DIR):
        try:
            date = int(re.match(pattern, filename).groupdict()['date'])
        except:
            continue

        util_log_info('NOW SAVING {}'.format(date))
        util_log_info('在数据库中的条数 {}'.format(coll.find({'report_date': date}).count()))
        try:
            filename = os.path.join(_CW_DIR, filename)
            df = HistoryFinancialReader().get_df(filename)

            # 修改columns的名称
            columns = df.columns.to_list()
            col = {}

            for name in columns[1:]:
                col[name] = '00{}'.format(name[3:])[-3:]

            df.rename(columns=col, inplace=True)

            data = util_to_json_from_pandas(
                df.reset_index().drop_duplicates(subset=['code', 'report_date']).sort_index()
            )
            util_log_info('即将更新的条数 {}'.format(len(data)))
            try:
                for d in data:
                    coll.update_one({'code': d['code'], 'report_date': d['report_date']}, {'$set': d}, upsert=True)

            except Exception as e:
                if isinstance(e, MemoryError):
                    coll.insert_many(data, ordered=True)
                elif isinstance(e, pymongo.bulk.BulkWriteError):
                    pass
        except Exception as e:
            util_log_info('似乎没有数据')

    util_log_info('SUCCESSFULLY SAVE/UPDATE FINANCIAL DATA')


if __name__ == '__main__':
    save_financial_files()