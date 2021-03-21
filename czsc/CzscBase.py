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
import re
from datetime import datetime
import json
import logging
import webbrowser

import numpy as np
import pandas as pd

from czsc.Fetch.mongo import FACTOR_DATABASE
from czsc.Fetch.tdx import get_bar
from czsc.Indicator import IndicatorSet
from czsc.Utils.echarts_plot import kline_pro
from czsc.Utils.logs import util_log_info
from czsc.Utils.trade_date import TradeDate, util_get_real_date, util_get_next_day
from czsc.Utils.transformer import DataEncoder


def identify_direction(v1, v2):
    if v1 > v2:  # 前面几根可能都是包含，这里直接初始赋值-1，上升趋势为正数
        direction = 1
    else:
        direction = -1
    return direction


def update_fx(bars, new_bars: list, fx_list: list, trade_date: list):
    """更新分型序列
    k线中有direction，fx中没有direction字段
        分型记对象样例：
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': -1, 低点用—1表示
              'value': 138.0,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
          }
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': +1, 高点用+1表示
              'value': 150.67,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
          }
        """
    assert len(bars) > 0
    bar = bars[-1].copy()

    if len(trade_date) > 1:
        if TradeDate(bar['date']) < TradeDate(trade_date[-1]):
            util_log_info('{} data is older than {} !'.format(bar['date'], trade_date[-1]))
            return

    trade_date.append(bar['date'])

    # 第1根K线没有方向,不需要任何处理
    if len(bars) < 2:
        new_bars.append(bar)
        return False

    last_bar = new_bars[-1]

    cur_h, cur_l = bar['high'], bar['low']
    last_h, last_l, last_dt = last_bar['high'], last_bar['low'], last_bar['date']

    # 处理过包含关系，只需要用一个值识别趋势
    direction = identify_direction(cur_h, last_h)

    # 第2根K线只需要更新方向
    if len(bars) < 3:
        bar.update(direction=direction)
        new_bars.append(bar)
        return False

    last_direction = last_bar.get('direction')

    # 没有包含关系，需要进行分型识别，趋势有可能改变
    if (cur_h > last_h and cur_l > last_l) or (cur_h < last_h and cur_l < last_l):
        new_bars.append(bar)
        # 分型识别
        if last_direction * direction < 0:

            bar.update(direction=direction)
            if direction < 0:
                fx = {
                    "date": last_bar['date'],
                    "fx_mark": 1,
                    "value": last_bar['high'],
                    "fx_start": new_bars[-3]['date'],  # 记录分型的开始和结束时间
                    "fx_end": bar['date'],
                    # "direction": bar['direction'],
                }
            else:
                fx = {
                    "date": last_bar['date'],
                    "fx_mark": -1,
                    "value": last_bar['low'],
                    "fx_start": new_bars[-3]['date'],  # 记录分型的开始和结束时间
                    "fx_end": bar['date'],
                    # "direction": bar['direction'],
                }
            fx_list.append(fx)
            return True
        bar.update(direction=last_direction + np.sign(last_direction))
        return False

    # 有包含关系，不需要进行分型识别，趋势不改变,direction数值增加
    bar.update(direction=last_direction + np.sign(last_direction))
    new_bars.pop(-1)  # 有包含关系的前一根数据被删除，这里是个技巧
    # 有包含关系，按方向分别处理,同时需要更新日期
    if last_direction > 0:
        if cur_h < last_h:
            bar.update(high=last_h, date=last_dt)
        if cur_l < last_l:
            bar.update(low=last_l)
    elif last_direction < 0:
        if cur_l > last_l:
            bar.update(low=last_l, date=last_dt)
        if cur_h > last_h:
            bar.update(high=last_h)
    else:
        logging.error('{} last_direction: {} is wrong'.format(last_dt, last_direction))
        raise ValueError

    new_bars.append(bar)
    return False


class XdList(object):
    """存放线段"""

    def __init__(self, bars, indicators, trade_date):
        # 传入的是地址，不要修改
        self.bars = bars
        self.indicators = indicators
        self.trade_date = trade_date

        # item存放数据元素
        self.xd_list = []  # 否则指向同一个地址
        # 低级别的中枢
        self.zs_list = []
        self.sig_list = []
        # next是低一级别的线段
        self.next = None
        # prev 指向高一级别的线段
        self.prev = None

    def __len__(self):
        return len(self.xd_list)

    def __getitem__(self, item):
        return self.xd_list[item]

    def __setitem__(self, key, value):
        self.xd_list[key] = value

    def append(self, value):
        self.xd_list.append(value)

    def update_zs(self):
        """
        {
              'zs_start': 进入段的起点
              'zs_end':  离开段的终点
              'ZG': 中枢高点,
              'ZD': 中枢低点,
              'GG': 中枢最低点,
              'DD': 中枢最高点，
              'xd_list': list[dict]
              'location': 中枢位置
          }
        """
        xd_list = self.xd_list

        if len(xd_list) < 3:
            return False

        zs_list = self.zs_list

        if len(zs_list) < 1:
            assert len(xd_list) < 4
            zg = xd_list[0] if xd_list[0]['fx_mark'] > 0 else xd_list[1]
            zd = xd_list[0] if xd_list[0]['fx_mark'] < 0 else xd_list[1]
            zs = {
                'ZG': zg,
                'ZD': zd,
                'GG': [zg],  # 初始用list储存，记录高低点的变化过程，中枢完成时可能会回退
                'DD': [zd],  # 根据最高最低点的变化过程可以识别时扩散，收敛，向上还是向下的形态
                'xd_list': xd_list[:2],
                'weight': 1,  # 记录中枢中段的数量
                'location': 0,  # 初始状态为0，说明没有方向， -1 表明下降第1个中枢， +2 表明上升第2个中枢
                'real_loc': 0  # 除去只有一段的中枢
            }
            zs_list.append(zs)
            return False

        # 确定性的笔参与中枢构建
        last_zs = zs_list[-1]
        xd = xd_list[-2]

        if TradeDate(last_zs['xd_list'][-1]['date']) >= TradeDate(xd['date']):
            # 已经计算过中枢
            return False

        if xd['fx_mark'] > 0:
            # 三卖 ,滞后，实际出现了一买信号
            if xd['value'] < last_zs['ZD']['value']:
                zs_end = last_zs['xd_list'].pop(-1)
                if zs_end['date'] == last_zs['DD'][-1]['date']:
                    last_zs['DD'].pop(-1)
                last_zs.update(
                    zs_end=zs_end,
                    weight=last_zs['weight'] - 1,
                    DD=last_zs['DD'],
                    real_loc=last_zs['real_loc'] + 1 if last_zs['weight'] == 2 else last_zs['real_loc']
                )

                zs = {
                    'zs_start': xd_list[-3],
                    'ZG': xd,
                    'ZD': zs_end,
                    'GG': [xd],
                    'DD': [zs_end],
                    'xd_list': [zs_end, xd],
                    'weight': 1,
                    'location': -1 if last_zs['location'] >= 0 else last_zs['location'] - 1,
                    'real_loc': -1 if last_zs['real_loc'] >= 0 else last_zs['real_loc'] - 1,
                }
                zs_list.append(zs)
                return True
            elif xd['value'] < last_zs['ZG']['value']:
                last_zs.update(ZG=xd)
            # 有可能成为离开段
            elif xd['value'] > last_zs['GG'][-1]['value']:
                last_zs['GG'].append(xd)
        elif xd['fx_mark'] < 0:
            # 三买，滞后，实际出现了一卖信号
            if xd['value'] > last_zs['ZG']['value']:
                zs_end = last_zs['xd_list'].pop(-1)
                if zs_end['date'] == last_zs['GG'][-1]['date']:
                    last_zs['GG'].pop(-1)
                last_zs.update(
                    zs_end=zs_end,
                    weight=last_zs['weight'] - 1,
                    GG=last_zs['GG'],
                    real_loc=last_zs['real_loc'] - 1 if last_zs['weight'] == 2 else last_zs['real_loc']
                )
                zs = {
                    'zs_start': xd_list[-3],
                    'ZG': zs_end,
                    'ZD': xd,
                    'GG': [zs_end],
                    'DD': [xd],
                    'xd_list': [zs_end, xd],
                    'weight': 1,
                    'location': 1 if last_zs['location'] <= 0 else last_zs['location'] + 1,
                    'real_loc': 1 if last_zs['real_loc'] <= 0 else last_zs['real_loc'] + 1,
                }
                zs_list.append(zs)
                return True
            elif xd['value'] > last_zs['ZD']['value']:
                last_zs.update(ZD=xd)
            # 有可能成为离开段
            elif xd['value'] < last_zs['DD'][-1]['value']:
                last_zs['DD'].append(xd)
        else:
            raise ValueError
        last_zs['xd_list'].append(xd)
        last_zs['weight'] = last_zs['weight'] + 1

        return False

    def update_xd_eigenvalue(self):
        trade_date = self.trade_date
        xd = self.xd_list[-1]
        last_xd = self.xd_list[-2]
        # xd.update(pct_change=(xd['value'] - last_xd['value']) / last_xd['value'])
        #
        start = trade_date.index(last_xd['date'])
        end = trade_date.index(xd['date'])
        kn = end - start + 1
        fx_mark = kn * np.sign(xd.get('fx_mark', xd.get('direction', 0)))
        dif = self.indicators.macd[end]['dif']
        macd = sum([x['macd'] for x in self.indicators.macd[start: end + 1] if fx_mark * x['macd'] > 0])
        xd.update(fx_mark=fx_mark, dif=dif, macd=macd)
        # xd.update(fx_mark=fx_mark, dif=dif, avg_macd=macd/kn)

    def update_xd(self):
        """更新笔分型序列
        分型记对象样例：
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': -8,  低点，负数，表示下降趋势持续的K线根数
              'value': 138.0,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
          }

         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 7, 高点， 正数，表示上升趋势持续的根数
              'value': 150.67,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
          }
        """
        # 至少3根同类型分型才可能出现线段，最后1根bi不确定，因此最后一段也不确定
        if self.next is None:
            self.next = XdList(self.bars, self.indicators, self.trade_date)

        bi_list = self.xd_list
        xd_list = self.next

        if len(bi_list) < 4:
            return False

        if len(xd_list) < 1:
            # 线段不存在，初始化线段，找4个点的最高和最低点组成线段
            bi_list = bi_list[:-1].copy()
            bi_list = sorted(bi_list, key=lambda x: x['value'], reverse=False)
            if TradeDate(bi_list[0]['date']) < TradeDate(bi_list[-1]['date']):
                xd_list.append(bi_list[0])
                xd_list.append(bi_list[-1])
            else:
                xd_list.append(bi_list[-1])
                xd_list.append(bi_list[0])

            xd_list.update_xd_eigenvalue()
            return True

        bi3 = bi_list[-3]
        xd = bi_list[-1].copy()
        last_xd = xd_list[-1]
        xd2 = xd_list[-2]

        # if xd['date'] > pd.to_datetime('2016-07-12'):
        #     print('test')

        # 非分型结尾段，直接替换成分型, 没有新增段，后续不需要处理，同一个端点确认
        if 'direction' in last_xd or xd['date'] == last_xd['date']:
            xd_list[-1] = xd  # 日期相等的情况是否已经在内存中修改过了？
            xd_list.update_xd_eigenvalue()
            return True

        # assert xd['date'] > last_xd['date']
        if TradeDate(xd['date']) <= TradeDate(last_xd['date']):
            util_log_info('The {} quotes bar input maybe wrong!'.format(xd['date']))

        if bi3['fx_mark'] > 0:
            # 同向延续
            if last_xd['fx_mark'] > 0 and xd['value'] > last_xd['value']:
                xd_list[-1] = xd
                xd_list.update_xd_eigenvalue()
                return True
            # 反向判断
            elif last_xd['fx_mark'] < 0:
                # 价格判断
                if xd['value'] > xd2['value']:
                    xd_list.append(xd)
                    xd_list.update_xd_eigenvalue()
                    return True
                # 出现三笔破坏线段，连续两笔，一笔比一笔高,寻找段之间的最高点
                elif TradeDate(bi3['date']) > TradeDate(last_xd['date']) and xd['value'] > bi3['value']:
                    index = -5
                    bi = bi_list[index]
                    # 连续两个高点没有碰到段前面一个低点
                    try:
                        if TradeDate(bi['date']) < TradeDate(last_xd['date']) and \
                                bi_list[index - 1]['value'] > bi3['value'] and \
                                bi_list[index]['value'] > xd['value']:
                            return False
                    except Exception as err:
                        pass
                        # util_log_info('Last xd {}:{}'.format(last_xd['date'], err))

                    while TradeDate(bi['date']) > TradeDate(last_xd['date']):
                        if xd['value'] < bi['value']:
                            xd = bi
                        index = index - 2
                        bi = bi_list[index]
                    xd_list.append(xd)
                    xd_list.update_xd_eigenvalue()
                    return True
        elif bi3['fx_mark'] < 0:
            # 同向延续
            if last_xd['fx_mark'] < 0 and xd['value'] < last_xd['value']:
                xd_list[-1] = xd
                xd_list.update_xd_eigenvalue()
                return True
            # 反向判断
            elif last_xd['fx_mark'] > 0:
                # 价格判断
                if xd['value'] < xd2['value']:
                    xd_list.append(xd)
                    xd_list.update_xd_eigenvalue()
                    return True
                # 出现三笔破坏线段，连续两笔，一笔比一笔低,将最低的一笔作为段的起点，避免出现最低点不是端点的问题
                elif TradeDate(bi3['date']) > TradeDate(last_xd['date']) and xd['value'] < bi3['value']:
                    index = -5
                    bi = bi_list[index]
                    # 连续两个个低点没有碰到段前面一高低点
                    try:
                        if TradeDate(bi['date']) < TradeDate(last_xd['date']) and \
                                bi_list[index - 1]['value'] < bi3['value'] and \
                                bi_list[index]['value'] < xd['value']:
                            return False
                    except Exception as err:
                        pass
                        # util_log_info('Last xd {}:{}'.format(last_xd['date'], err))

                    while TradeDate(bi['date']) > TradeDate(last_xd['date']):
                        if xd['value'] > bi['value']:
                            xd = bi
                        index = index - 2
                        bi = bi_list[index]
                    xd_list.append(xd)
                    xd_list.update_xd_eigenvalue()
                    return True
        return False

    def update_sig(self):
        """
        线段更新后调用，判断是否出现买点
        """
        if len(self.zs_list) < 1:
            return False

        zs = self.zs_list[-1]
        xd = self.xd_list[-1]

        sig = {
            'date': self.bars[-1]['date'],
            'real_loc': zs['real_loc'],
            'location': zs['location'],
            'weight': zs['weight'],
            # 'fx_mark': xd['fx_mark'],
            # 'last_mark': last_xd['fx_mark'],
            # 'time_ratio': abs(xd['fx_mark'] / last_xd['fx_mark']) * 100,
            # 'pct_change': xd['pct_change'] * 100,
            # 'macd': xd['macd'],
            # 'avg_macd': xd['avg_macd'],
        }

        if xd['fx_mark'] > 0:  # 上升趋势
            # sig.update(GG_macd=zs['GG'][-1].get('macd', np.nan), GG_avg_macd=zs['GG'][-1].get('avg_macd', np.nan))
            # if zs['location'] > 0 and zs.get('zs_start', False):
            #     sig.update(start_macd=zs['zs_start']['macd'], start_avg_macd=zs['zs_start']['avg_macd'])

            sig.update(boll=self.indicators.boll[-1].get('UB', np.nan) / self.bars[-1]['high'] * 100 - 100)

            if xd['value'] > zs['GG'][-1]['value']:
                xd_mark = -1  # 如果weight=1, 背驰，有可能1卖
                # resistance = np.nan
                # support = zs['GG'][-1]['value'] / xd['value'] - 1
            elif xd['value'] > zs['ZG']['value']:
                xd_mark = -2  # 如果weight=1, 背驰，有可能2卖
                # resistance = zs['GG'][-1]['value'] / xd['value'] - 1
                # support = zs['ZG']['value'] / xd['value'] - 1
            elif xd['value'] > zs['ZD']['value']:
                xd_mark = -2.5
                # resistance = zs['ZG']['value'] / xd['value'] - 1
                # support = zs['ZD']['value'] / xd['value'] - 1
            elif xd['value'] > zs['DD'][-1]['value']:
                xd_mark = -3  # 三卖
                # resistance = zs['ZD']['value'] / xd['value'] - 1
                # support = zs['DD'][-1]['value'] / xd['value'] - 1
            else:
                xd_mark = -4  # 三卖
                # resistance = zs['DD'][-1]['value'] / xd['value'] - 1
                # support = np.nan

        elif xd['fx_mark'] < 0:  # 下降趋势
            # sig.update(DD_macd=zs['DD'][-1].get('macd', np.nan), DD_avg_macd=zs['DD'][-1].get('avg_macd', np.nan))
            # if zs['location'] < 0 and zs.get('zs_start', False):
            #     sig.update(start_macd=zs['zs_start']['macd'], start_avg_macd=zs['zs_start']['avg_macd'])

            sig.update(boll=100 - self.indicators.boll[-1].get('LB', np.nan) / self.bars[-1]['low'] * 100)

            if xd['value'] > zs['GG'][-1]['value']:
                xd_mark = 4  # 三买
                # resistance = np.nan
                # support = zs['GG'][-1]['value'] / xd['value'] - 1
            elif xd['value'] > zs['ZG']['value']:
                xd_mark = 3
                # resistance = zs['GG'][-1]['value'] / xd['value'] - 1
                # support = zs['ZG']['value'] / xd['value'] - 1
            elif xd['value'] > zs['ZD']['value']:
                xd_mark = 2.5
                # resistance = zs['ZG']['value'] / xd['value'] - 1
                # support = zs['ZD']['value'] / xd['value'] - 1
            elif xd['value'] > zs['DD'][-1]['value']:
                xd_mark = 2  # 如果weight=1, 背驰，有可能2买
                # resistance = zs['ZD']['value'] / xd['value'] - 1
                # support = zs['DD'][-1]['value'] / xd['value'] - 1
            else:
                xd_mark = 1  # 如果weight=1, 背驰，有可能1买
                # resistance = zs['DD'][-1]['value'] / xd['value'] - 1
                # support = np.nan
        else:
            raise ValueError

        # sig.update(xd_mark=xd_mark, support=support * 100, resistance=resistance * 100)
        sig.update(xd_mark=xd_mark)

        # if xd_mark not in [1, -1]:  # 只有1买-1卖需要计算macd背离的情况
        #     self.sig_list.append(sig)
        #     return

        xd_list = zs['xd_list'][-2::-2]
        xd_list.reverse()

        compare_dif = None
        compare_macd = None

        # 没有进入段，或者与进入段反向
        if xd_list[0]['fx_mark'] * xd['fx_mark'] < 0 or 'zs_start' not in zs:
            if xd['fx_mark'] < 0:
                peak_date = zs['GG'][-1]['date']
            else:
                peak_date = zs['DD'][-1]['date']

            for _xd in xd_list:
                if _xd['date'] > peak_date:
                    compare_dif = _xd.get('dif')
                    compare_macd = _xd.get('macd')

                    break
        else:
            compare_dif = xd_list[0].get('dif')
            compare_macd = xd_list[0].get('macd')

        dif = xd.get('dif')
        macd = xd.get('macd')

        direction = np.sign(xd['fx_mark'])

        if compare_dif and dif:
            if dif * direction > compare_dif * direction:
                sig.update(dif=-1)
            else:
                sig.update(dif=1)

        if compare_macd and macd:
            if macd * direction > compare_macd * direction:
                sig.update(macd=-1)
            else:
                sig.update(macd=1)

        self.sig_list.append(sig)

    def update(self):

        self.update_zs()

        # 计算对应买卖点
        self.update_sig()

        return self.update_xd()


def update_bi(new_bars: list, fx_list: list, bi_list: XdList, trade_date: list):
    """更新笔序列
    笔标记对象样例：和分型标记序列结构一样
     {
         'date': Timestamp('2020-11-26 00:00:00'),
         'code': code,
          'fx_mark': 'd',
          'value': 138.0,
          'fx_start': Timestamp('2020-11-25 00:00:00'),
          'fx_end': Timestamp('2020-11-27 00:00:00'),
      }

     {
         'date': Timestamp('2020-11-26 00:00:00'),
         'code': code,
          'fx_mark': 'g',
          'value': 150.67,
          'fx_start': Timestamp('2020-11-25 00:00:00'),
          'fx_end': Timestamp('2020-11-27 00:00:00'),
      }

      return: True 笔的数据出现更新，包括新增笔或者笔的延续
    """
    # 每根k线都要对bi进行判断
    bar = new_bars[-1].copy()

    if TradeDate(bar['date']) < TradeDate(trade_date[-1]):
        # 包含的K线，不会改变bi的状态，不需要处理
        return False

    if len(fx_list) < 2:
        return False

    bi = fx_list[-1].copy()

    # 没有笔时.最开始两个分型作为第一笔，增量更新时从数据库取出两个端点构成的笔时确定的
    if len(bi_list) < 1:
        bi2 = fx_list[-2].copy()
        bi_list.append(bi2)
        bi_list.append(bi)
        bi_list.update_xd_eigenvalue()
        return False

    last_bi = bi_list[-1]
    bar.update(value=bar['high'] if bar['direction'] > 0 else bar['low'])

    # if bar['date'] > pd.to_datetime('2020-09-08'):
    #     print('error')

    # k 线确认模式，当前K线的日期比分型K线靠后，说明进来的数据时K线
    if TradeDate(bar['date']) > TradeDate(bi['fx_end']):
        if 'direction' not in last_bi:  # bi的结尾是分型
            # 趋势延续替代,首先确认是否延续, 由于处理过包含，高低点可能不正确，反趋势的极值点会忽略
            # 下一根继续趋势，端点后移，如果继续反趋势，该点忽略
            # todo 处理过包含的bar，有一个判断是多余的，直接用bar['value] 参与判断
            if (last_bi['fx_mark'] > 0 and bar['high'] > last_bi['value']) \
                    or (last_bi['fx_mark'] < 0 and bar['low'] < last_bi['value']):
                bi_list[-1] = bar
                bi_list.update_xd_eigenvalue()
                return True

            try:
                kn_inside = trade_date.index(bar['date']) - trade_date.index(last_bi['fx_end']) - 1
            except:
                print('error')

            # todo 至少2根k线， 时间确认必须被和前一笔方向相反，会出现端点不是极值点的情况
            if kn_inside > 1 and bar['direction'] * last_bi['fx_mark'] < 0:
                # 寻找同向的第一根分型
                index = -1
                while TradeDate(bi['date']) > TradeDate(last_bi['date']):
                    if bar['direction'] * bi['fx_mark'] > 0:
                        break
                    index = index - 1
                    bi = fx_list[index]

                if (bar['direction'] * bi['fx_mark'] > 0) \
                        and (np.sign(bar['direction']) * bar['value'] < bi['fx_mark'] * bi['value']):
                    bi['fx_end'] = bar['date']  # 影响似乎不大？
                    bi_list.append(bi)
                else:
                    bi_list.append(bar)
                bi_list.update_xd_eigenvalue()
                return True

            # 只有一个端点，没有价格确认
            if len(bi_list) < 2:
                return False

            # 价格确认
            # todo 处理过包含的bar，有一个判断是多余的，直接用bar['value] 参与判断
            if (last_bi['fx_mark'] < 0 and bar['high'] > bi_list[-2]['value']) \
                    or (last_bi['fx_mark'] > 0 and bar['low'] < bi_list[-2]['value']):
                bi_list.append(bar)
                bi_list.update_xd_eigenvalue()
                return True

        else:  # 原有未出现分型笔的延续
            assert bar['direction'] * last_bi['direction'] > 0
            # if bar['direction'] * last_bi['direction'] < 0:
            #     print('error')
            #     return False
            bi_list[-1] = bar
            bi_list.update_xd_eigenvalue()
            return True
        return False

    # 非分型结尾笔，直接替换成分型, 没有新增笔，后续不需要处理，同一个端点确认
    if 'direction' in last_bi or bi['date'] == last_bi['date']:
        bi_list[-1] = bi
        bi_list.update_xd_eigenvalue()
        return True

    # fx_end处理，分型处理完后，因为分型确认滞后，所以还需要对fx_end 也就是当前K线进行处理，否则会出现缺失或者识别滞后的问题
    # 由于时分型，只需要判断延续的问题，因此K线的方向要和上一笔一致
    def handle_fx_end():
        assert bar['date'] == bi['fx_end']
        if bar['direction'] * last_bi['fx_mark'] < 0:
            return False

        if last_bi['fx_mark'] * bar['value'] > last_bi['fx_mark'] * last_bi['value']:
            bi_list[-1] = bar
            bi_list.update_xd_eigenvalue()
            return True

    # 分型处理，连续高低点处理，只判断是否后移,没有增加笔
    # bi的fx_mark不一定为+1或者-1，因为要用sign函数取符号
    # todo 为什么用 and 连接两个 if 结果错误
    if last_bi['fx_mark'] * bi['fx_mark'] > 0:
        if np.sign(last_bi['fx_mark']) * last_bi['value'] < bi['fx_mark'] * bi['value']:
            bi_list[-1] = bi
            bi_list.update_xd_eigenvalue()
            return True
    else:
        # 笔确认是条件1、时间破坏，两个不同分型间至少有一根K线，2、价格破坏，向下的一笔破坏了上一笔的低点
        kn_inside = trade_date.index(bi['fx_start']) - trade_date.index(last_bi['fx_end']) - 1

        if kn_inside > 0:  # 两个分型间至少有1根k线，端点有可能不是高低点
            index = -2
            while TradeDate(fx_list[index]['date']) > TradeDate(last_bi['date']):
                # 分析的fx_mark取值为-1和+1
                if (bi['fx_mark'] * fx_list[index]['fx_mark'] > 0) \
                        and (bi['fx_mark'] * bi['value'] < fx_list[index]['fx_mark'] * fx_list[index]['value']):
                    bi = fx_list[index].copy()
                    # 分型结尾不变
                    bi['fx_end'] = fx_list[-1]['fx_end']

                index = index - 1

            bi_list.append(bi)
            bi_list.update_xd_eigenvalue()
            return True

        # 只有一个端点，没有价格确认
        if len(bi_list) < 2:
            return False

        # 价格确认
        # todo 处理过包含的bar，有一个判断是多余的，直接用bar['value] 参与判断
        if (bi['fx_mark'] > 0 and bi['value'] > bi_list[-2]['value']) \
                or (bi['fx_mark'] < 0 and bi['value'] < bi_list[-2]['value']):
            bi_list.append(bi)
            bi_list.update_xd_eigenvalue()
            return True

    return handle_fx_end()


class CzscBase:
    def __init__(self):
        # self.freq = freq
        # assert isinstance(code, str)
        # self.code = code.upper()

        self.trade_date = []  # 用来查找索引
        self.bars = []
        self.indicators = IndicatorSet(self.bars)
        # self.indicators = None
        self.new_bars = []
        self.fx_list = []
        self.xd_list = XdList(self.bars, self.indicators, self.trade_date)  # bi作为线段的head
        self.sig_list = []

    def update(self):
        # 有包含关系时，不可能有分型出现，不出现分型时才需要
        self.indicators.update()

        try:
            update_fx(bars=self.bars, new_bars=self.new_bars, fx_list=self.fx_list, trade_date=self.trade_date)
        except:
            print('error')

        if not update_bi(
                new_bars=self.new_bars, fx_list=self.fx_list, bi_list=self.xd_list, trade_date=self.trade_date
        ):
            return

        # 新增确定性的笔才处理段
        xd_list = self.xd_list
        result = True
        index = 0
        while result:
            result = xd_list.update()

            # 计算对应买卖点
            if len(xd_list.sig_list) > 0:
                signal = xd_list.sig_list[-1]
                if index == 0:
                    signal.update(xd=0)
                    self.sig_list.append(signal)
                else:
                    if xd_list.zs_list[-1]['location'] != 0:
                        last_sig = self.sig_list[-1]
                        last_sig.update(xd=index, xd_mark=signal['xd_mark'])
                        last_sig['real_loc'] = signal['real_loc']
                        last_sig['location'] = signal['location']
                        last_sig['weight'] = signal['weight']
                        # if signal['xd_mark'] in [1, -1]:
                        last_sig['dif{}'.format(index)] = signal.get('dif')
                        last_sig['macd{}'.format(index)] = signal.get('macd')

                    # else:
                    #     util_log_info('High level xd {} == low level xd {}'.format(index, index - 1))

            temp_list = xd_list
            xd_list = xd_list.next
            xd_list.prev = temp_list
            index = index + 1

    #  必须实现,每次输入一个行情数据，然后调用update看是否需要更新
    def on_bar(self, bar):
        """
        输入数据格式
        Index(['open', 'high', 'low', 'close', 'amount', 'volume', 'date', 'code'], dtype='object')
        'date' 未 timestamp  volume用来画图
        """
        raise NotImplementedError


class CzscMongo(CzscBase):
    def __init__(self, code='rul8', start=None, end=None, freq='day', exchange=None):
        # 只处理一个品种
        super().__init__()
        self.code = code
        self.freq = freq
        self.exchange = exchange

        # self._bi_list = fetch_future_bi_day(self.code, limit=2, format='dict')
        self._bi_list = []
        self.old_count = len(self._bi_list)
        if len(self._bi_list) > 0:
            # self.fx_list = self._bi_list
            start = self._bi_list[-1]['fx_end']
        elif start is None:
            start = '1990-01-01'

        self.data = get_bar(code, start=start, end=end, freq=freq, exchange=exchange)
        # self.data = get_bar(code, start, end='2020-12-09', freq=freq, exchange=exchange)

    def draw(self, chart_path=None):
        if len(self.bars) < 1:
            return

        chart = kline_pro(
            kline=self.bars, fx=self.fx_list,
            bs=[], xd=self.xd_list,
            # title=self.code+'_'+self.freq, width='1520px', height='580px'
            title=self.code + '_' + self.freq, width='2540px', height='850px'
        )

        if not chart_path:
            chart_path = 'E:\\signal\\{}_{}.html'.format(self.code, self.freq)
        chart.render(chart_path)
        webbrowser.open(chart_path)

    def on_bar(self, bar):
        """
        bar 格式
        date 默认为 Timestamp，主要时画图函数使用
        """
        bar = bar.to_dict()
        # if 'trade' in bar:
        #     bar['vol'] = bar.pop('trade')
        # bar['date'] = pd.to_datetime(bar['date'])
        self.bars.append(bar)

        try:
            self.update()
        except Exception as error:
            util_log_info(error)

    def run(self, start=None, end=None):
        if self.data is None or self.data.empty:
            util_log_info('{} {} quote data is empty'.format(self.code, self.freq))
            return

        self.data.apply(self.on_bar, axis=1)
        # self.save()

    def save(self, collection=FACTOR_DATABASE.future_bi_day):
        try:
            logging.info('Now Saving Future_BI_DAY==== {}'.format(str(self.code)))
            code = self.code

            old_count = self.old_count
            new_count = len(self._bi_list)

            # 更新的数据，最后一个数据是未确定数据
            update_count = new_count - old_count

            if update_count < 2:
                return

            bi_list = self._bi_list[old_count:new_count - 1]

            start = bi_list[0]['date']
            end = bi_list[-1]['date']
            logging.info(
                'UPDATE_Future_BI_DAY \n Trying updating {} from {} to {}'.format(code, start, end),
            )

            collection.insert_many(bi_list)
        except Exception as error:
            print(error)

    def save_sig(self, collection=FACTOR_DATABASE.czsz_sig_day):
        try:
            logging.info('Now Saving CZSC_SIG_DAY==== {}'.format(str(self.code)))
            code = self.code

            xd = self.xd_list
            index = 0
            sig = []
            while xd:
                df = pd.DataFrame(xd.sig_list)
                df['xd'] = index
                df['code'] = code
                df['exchange'] = self.exchange
                sig.append(df)
                xd = xd.next
                index = index + 1

            sig_df = pd.concat(sig).set_index(['date', 'xd']).sort_index()

            old_count = self.old_count
            new_count = len(self._bi_list)

            # 更新的数据，最后一个数据是未确定数据
            update_count = new_count - old_count

            if update_count < 2:
                return

            bi_list = self._bi_list[old_count:new_count - 1]

            start = bi_list[0]['date']
            end = bi_list[-1]['date']
            logging.info(
                'UPDATE_Future_BI_DAY \n Trying updating {} from {} to {}'.format(code, start, end),
            )

            collection.insert_many(bi_list)
        except Exception as error:
            print(error)

    def to_csv(self):

        if len(self.sig_list) < 1:
            return

        sig_df = pd.DataFrame(self.sig_list).set_index('date')
        filename = 'E:\\signal\\{}_{}_sig.csv'.format(self.code, self.freq)
        sig_df.to_csv(filename)

    def to_df(self):
        xd = self.xd_list
        index = 0
        sig = []
        while xd:
            df = pd.DataFrame(xd.sig_list)
            df['xd'] = index
            df['code'] = self.code
            df['exchange'] = self.exchange
            sig.append(df)
            xd = xd.next
            index = index + 1

        try:
            sig_df = pd.concat(sig).set_index(['date', 'xd']).sort_index()
            return sig_df
        except:
            util_log_info("{} signal is empty!".format(self.code))
            return pd.DataFrame()

    def to_json(self):
        xd = self.xd_list

        if len(xd) < 1:
            return

        index = 0
        data = []
        while xd:
            data.append(
                {
                    'xd{}'.format(index): xd.xd_list,
                    'zs{}'.format(index): xd.zs_list,
                    'sig{}'.format(index): xd.sig_list
                }
            )
            xd = xd.next
            index = index + 1

        with open("{}_{}.json".format(self.code, self.freq), "w") as write_file:
            json.dump(data, write_file, indent=4, sort_keys=True, cls=DataEncoder)


def calculate_bs_signals(security_df: pd.DataFrame, last_trade_date=None):
    sig_list = []

    if last_trade_date is None:
        last_trade_date = util_get_real_date(datetime.today().strftime('%Y-%m-%d'))

    last_trade_time = pd.to_datetime(util_get_next_day(last_trade_date))
    last_trade_date = pd.to_datetime(last_trade_date)

    index = 0

    for code, item in security_df.iterrows():
        exchange = item['exchange']
        util_log_info("============={} {} Signal==========".format(code, exchange))
        try:
            czsc_day = CzscMongo(code=code, end=last_trade_date, freq='day', exchange=exchange)
        except Exception as error:
            util_log_info("{} : {}".format(code, error))
            continue

        if len(czsc_day.data) < 1:
            util_log_info("==========={} {} 0 Quotes==========".format(code, exchange))
            continue

        if czsc_day.data.iloc[-1]['date'] < last_trade_date:
            util_log_info(
                "=={} {} last trade date {}==".format(
                    code, exchange, czsc_day.data.iloc[-1]['date'].strftime('%Y-%m-%d'))
            )
            continue

        czsc_day.run()
        sig_day_list = czsc_day.sig_list

        if len(sig_day_list) < 1:
            continue

        last_day_sig = sig_day_list[-1]

        if last_day_sig['date'] < last_trade_date:
            util_log_info(
                "===={} {} last Signal {}====".format(code, exchange, last_day_sig['date'].strftime('%Y-%m-%d'))
            )
            continue

            # 取周线级别的起点
        xd1_list = czsc_day.xd_list.next

        if len(xd1_list) < 1:
            continue

        last_xd = xd1_list.xd_list[-1]
        xd1_mark = last_day_sig['xd_mark']

        if last_xd['fx_mark'] * xd1_mark < 0:
            if len(xd1_list) < 2:
                continue
            last_xd = xd1_list.xd_list[-2]

        start = last_xd['fx_start']

        czsc_min = CzscMongo(code=code, start=start, end=last_trade_time, freq='5min', exchange=exchange)

        if len(czsc_min.data) < 1:
            util_log_info("========={} {} 0 5min Quotes========".format(code, exchange))
            continue

        if czsc_min.data.iloc[-1]['date'] < last_trade_date:
            util_log_info(
                "==Please Update {} {} 5min Quotes from {}==".format(
                    code, exchange, czsc_day.data.iloc[-1]['date'].strftime('%Y-%m-%d'))
            )
            continue

        czsc_min.run()

        sig_min_list = czsc_min.sig_list

        if len(sig_min_list) < 1:
            continue

        last_min_sig = sig_min_list[-1]

        if last_min_sig['date'] < last_trade_date:
            continue

        df = pd.DataFrame(sig_min_list).set_index('date')
        # df = df[df.index > last_trade_date]  # 5分钟数据有可能缺失，造成实际没数据
        # df = df[df['xd_mark'] * xd1_mark > 0]  # 只保留和day同向的买卖点

        bar_df = pd.DataFrame(czsc_min.bars).set_index('date')
        bar_df = bar_df[bar_df.index > last_trade_date]

        if xd1_mark > 0:
            idx = bar_df['low'].idxmin()
        else:
            idx = bar_df['high'].idxmax()

        if df.empty:
            util_log_info("===Please Download {} {} 5min Data===".format(code, exchange))
            continue

        # df = df.sort_values(by=['xd', 'xd_mark', 'location'], ascending=[False, xd1_mark > 0, xd1_mark > 0])

        # last_min_sig = df.iloc[0].to_dict()
        try:
            last_min_sig = df.loc[idx].to_dict()
        except:
            util_log_info("{} {} Have a opposite Signal=======".format(code, exchange))
            continue

        if last_min_sig['xd'] < 2 or last_min_sig['xd_mark'] not in [1, -1]:
            util_log_info("==={} xd:{}, xd_mark:{}===".format(code, last_min_sig['xd'], last_min_sig['xd_mark']))
            continue

        idx = 1

        last_min_sig.update(macd=last_min_sig.get('dif') + last_min_sig.get('macd'))

        while 'dif{}'.format(idx) in last_min_sig:
            last_min_sig.update(macd=last_min_sig.get('macd') +
                                     last_min_sig.get('dif{}'.format(idx)) + last_min_sig.get('macd{}'.format(idx)))
            idx = idx + 1

        for key in last_min_sig:
            last_day_sig[key + '_min'] = last_min_sig[key]



        last_day_sig['start'] = start

        if item['instrument'] == 'future':
            amount = czsc_day.bars[-1]['volume']
        elif exchange in ['hkconnect']:
            amount = czsc_day.bars[-1]['hk_stock_amount']
        else:
            amount = czsc_day.bars[-1]['amount']

        last_day_sig.update(amount=amount, code=code, exchange=exchange)

        sig_list.append(last_day_sig)

        index = index + 1
        util_log_info("==={:=>4d}. {} {} Have a Signal=======".format(index, code, exchange))

    if len(sig_list) < 1:
        util_log_info("========There are 0 Signal=======")
        return None

    df = pd.DataFrame(sig_list)
    columns = df.columns.to_list()
    # order = df.columns.sort_values(ascending=False).drop(['date', 'location', 'location_min', 'exchange'])

    order = [
        'code',
        'xd', 'real_loc', 'xd_mark', 'weight', 'boll', 'dif', 'macd',
        'start',
        'xd_min', 'real_loc_min', 'xd_mark_min', 'weight_min', 'boll_min', 'macd_min',
        'amount',
    ]

    idx = 1
    day_index = order.index('start')

    while 'dif{}'.format(idx) in columns:
        order.insert(day_index, 'dif{}'.format(idx))
        if 'macd{}'.format(idx) in columns:
            day_index = day_index + 1
            order.insert(day_index, 'macd{}'.format(idx))

        day_index = day_index + 1
        idx = idx + 1

    idx = 1
    min_index = order.index('amount')

    # while 'dif{}_min'.format(idx) in columns:
    #     order.insert(min_index, 'dif{}_min'.format(idx))
    #     if 'macd{}_min'.format(idx) in columns:
    #         min_index = min_index + 1
    #         order.insert(min_index, 'macd{}_min'.format(idx))
    #
    #     min_index = min_index + 1
    #     idx = idx + 1

    df = df[order].sort_values(by=['macd_min', 'xd', 'real_loc'], ascending=[False, False, True])

    util_log_info("===There are {} Signal=======".format(index))
    return df


def main_signal(last_trade_date=None, security_blocks=None):
    from czsc.Fetch.tdx import SECURITY_DATAFRAME

    security_classes = [
        {
            'name': 'future',
            'exchange': ['czce', 'dce', 'shfe', 'cffex'], 'instrument': ['future'],  # 'code': "^\w+L[89]$"
        },
        {'name': 'stock', 'exchange': ['sse', 'szse'], 'instrument': ['stock']},
        {'name': 'index', 'exchange': ['sse', 'szse', 'csindex', 'sse szse'], 'instrument': ['index']},
        {'name': 'convertible', 'exchange': ['sse', 'szse'], 'instrument': ['convertible']},
        {'name': 'ETF', 'exchange': ['sse', 'szse'], 'instrument': ['ETF']},
        {'name': 'hkconnect', 'exchange': ['hkconnect'], 'instrument': ['stock']},
    ]

    def inst_filter(security):
        for security_class in security_classes:
            if security['exchange'] in security_class['exchange'] and \
                    security['instrument'] in security_class['instrument']:
                if 'code' in security_class:
                    if re.match(security_class['code'], security.name) is None:
                        continue
                return security_class['name']

        return np.nan

    if last_trade_date is None:
        last_trade_date = pd.to_datetime(util_get_real_date(datetime.today().strftime('%Y-%m-%d')))

    security_df = SECURITY_DATAFRAME
    security_df['class'] = security_df.apply(inst_filter, axis=1)
    security_df = security_df[security_df['last_modified'].apply(lambda x: x >= last_trade_date)]

    for class_name in security_blocks:
        df = calculate_bs_signals(security_df[security_df['class'] == class_name], last_trade_date)

        if df is None or df.empty:
            continue

        if class_name == 'stock':
            filename = 'E:\\signal\\scores.csv'
            scores_df = pd.read_csv(filename)
            scores_df['code'] = scores_df['code'].apply(lambda x: "{:0>6d}".format(x))
            scores_df.set_index('code', inplace=True)
            df = df.join(scores_df, on='code')
        df.to_csv('E:\\signal\\{}_signal_{}.csv'.format(class_name, last_trade_date.strftime('%Y%m%d')), index=False)


def main_single():
    code = 'apl8'
    exchange = 'szse'
    end = '2021-08-27'
    czsc_day = CzscMongo(code=code, end=end, freq='day', exchange=exchange)
    czsc_day.run()
    xd1_list = czsc_day.xd_list.next
    xd1_mark = xd1_list.sig_list[-1]['xd_mark']
    last_xd = xd1_list.xd_list[-1]
    if last_xd['fx_mark'] * xd1_mark < 0:
        last_xd = xd1_list.xd_list[-2]

    start = last_xd['fx_start']

    czsc_day.draw()
    czsc_day.to_csv()
    # czsc_day.to_json()

    czsc_min = CzscMongo(code=code, start=start, end=end, freq='5min', exchange=exchange)
    czsc_min.run()
    czsc_min.draw()
    czsc_min.to_csv()
    # # czsc_min.to_json()


if __name__ == '__main__':
    # main_consumer()
    # last_trade_date = pd.to_datetime('2021-03-10')
    last_trade_date = None
    main_signal(
        security_blocks=['future'],
        # security_blocks=['future', 'stock', 'convertible', 'ETF', 'index'],
        # security_blocks=['hkconnect'],
        # security_blocks=['stock'],
        last_trade_date=last_trade_date
    )
    # main_single()
