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
import json
import logging
import webbrowser

import datetime
import pandas as pd
import numpy as np
import pymongo

from czsc.ClPubSub.consumer import Subscriber
from czsc.ClPubSub.producer import Publisher

from czsc.Fetch.mongo import FACTOR_DATABASE, fetch_future_bi_day
from czsc.Fetch.tdx import get_bar
from czsc.ClEngine.ClThread import ClThread
from czsc.Utils.echarts_plot import kline_pro
from czsc.Utils.logs import util_log_info
from czsc.Utils.trade_date import util_get_next_day, util_get_trade_gap


class ClSimBar(ClThread):
    """
    从数据库取数据，模拟像队列发送消息
    """

    def __init__(self, code='rbl8', freq='day', start=None, end=None):
        # 生产行情数据
        super().__init__()
        if isinstance(code, str):
            self.code = [code.upper()]
        elif isinstance(code, list):
            self.code = [c.upper() for c in code]
        else:
            logging.info('Wrong code format!')

        self.freq = freq

        start = '1990-01-01' if start is None else start
        end = str(datetime.date.today()) if end is None else end
        self._start = str(start)[0:10]
        self._end = str(end)[0:10]

        # # todo 根据字母特征识别，要么全是期货，要么全是股票，跨市场跨品种不支持，包装城函数，根据code去分发
        # self.market_type = MARKET_TYPE.FUTURE_CN if re.search(
        #     r'[a-zA-z]+', self.code[0]) else MARKET_TYPE.STOCK_CN

        self.pro = {}

    def publish_bar(self, item):
        code = item['code']
        # print('send {} {} quotation!'.format(item['date'], code))
        producer = self.pro.get(code, Publisher(exchange='bar_{}_{}'.format(self.frq, code)))
        bar = item.to_dict()
        # todo 对输入字符进行模糊判断，转换为统一的频率标识,日期时间数据统一用datetime表示
        bar.update(date=item['date'].strftime("%Y-%m-%d %H:%M:%S"))
        producer.pub(json.dumps(bar))
        # time.sleep(1)

    def run(self):
        # todo 从多个数据源取数据
        data = fetch_future_day(self.code, self._start, self._end)
        data.apply(self.publish_bar, axis=1)

        for code in self.code:
            producer = self.pro.get(code, Publisher(exchange='bar_{}_{}'.format(self.frq, code)))
            producer.pub('TERMINATED')


def identify_direction(v1, v2):
    if v1 > v2:  # 前面几根可能都是包含，这里直接初始赋值-1
        direction = 1
    else:
        direction = -1
    return direction


def update_fx(bars, new_bars: list, fx_list: list, trade_date: list):
    """更新分型序列
        分型记对象样例：
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'd',
              'value': 138.0,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': >=1, 趋势持续的K线根数
          }
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'g',
              'value': 150.67,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': <=-1,
          }
        """
    assert len(bars) > 0
    bar = bars[-1].copy()

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
                    "fx_mark": -1,
                    "value": last_bar['high'],
                    "fx_start": new_bars[-2]['date'],  # 记录分型的开始和结束时间
                    "fx_end": bar['date'],
                    # "direction": bar['direction'],
                }
            else:
                fx = {
                    "date": last_bar['date'],
                    "fx_mark": 1,
                    "value": last_bar['low'],
                    "fx_start": new_bars[-2]['date'],  # 记录分型的开始和结束时间
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

    def __init__(self, xd_list=[], zs_list=[]):
        # item存放数据元素
        self.xd_list = xd_list.copy()  # 否则指向同一个地址
        # 低级别的中枢
        self.zs_list = zs_list.copy()
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
              'bi_list': list[dict]
              'location': 中枢位置
          }
        """
        xd_list = self.xd_list
        zs_list = self.zs_list

        if len(zs_list) < 1:
            assert len(xd_list) < 4
            zg = xd_list[0] if xd_list[0]['fx_mark'] == 'g' else xd_list[1]
            zd = xd_list[0] if xd_list[0]['fx_mark'] == 'd' else xd_list[1]
            zs = {
                'ZG': zg,
                'ZD': zd,
                'GG': [zg],  # 初始用list储存，记录高低点的变化过程，中枢完成时可能会回退
                'DD': [zd],  # 根据最高最低点的变化过程可以识别时扩散，收敛，向上还是向下的形态
                'bi_list': xd_list[:2],
                'location': 0  # 初始状态为0，说明没有方向， -1 表明下降第1割中枢， +2 表明上升第2个中枢
            }
            zs_list.append(zs)
            return False

        # 确定性的笔参与中枢构建
        last_zs = zs_list[-1]
        bi = xd_list[-2]

        if last_zs['bi_list'][-1]['date'] == bi['date']:
            # 已经计算过中枢
            return False

        if bi['fx_mark'] < 0:
            # 三卖 ,滞后，实际出现了一买信号
            if bi['value'] < last_zs['ZD']['value']:
                zs_end = last_zs['bi_list'].pop(-1)
                last_zs.update(
                    zs_end=zs_end,
                    DD=last_zs['DD'].pop(-1) if zs_end['date'] == last_zs['DD'][-1]['date'] else last_zs['DD']
                )

                zs = {
                    'zs_start': xd_list[-4],
                    'ZG': bi,
                    'ZD': zs_end,
                    'GG': [bi],
                    'DD': [zs_end],
                    'bi_list': [zs_end, bi],
                    'location': -1 if last_zs['location'] >= 0 else last_zs['location'] - 1
                }
                zs_list.append(zs)
                return True
            elif bi['value'] < last_zs['ZG']['value']:
                last_zs.update(ZG=bi)
            # 有可能成为离开段
            elif bi['value'] > last_zs['GG'][-1]['value']:
                last_zs['GG'].append(bi)
        elif bi['fx_mark'] > 0:
            # 三买，滞后，实际出现了一卖信号
            if bi['value'] > last_zs['ZG']['value']:
                zs_end = last_zs['bi_list'].pop(-1)
                last_zs.update(
                    zs_end=zs_end,
                    GG=last_zs['GG'].pop(-1) if zs_end['date'] == last_zs['GG'][-1]['date'] else last_zs['GG']
                )
                zs = {
                    'zs_start': xd_list[-4],
                    'ZG': zs_end,
                    'ZD': bi,
                    'GG': [zs_end],
                    'DD': [bi],
                    'bi_list': [zs_end, bi],
                    'location': 1 if last_zs['location'] <= 0 else last_zs['location'] - 1
                }
                zs_list.append(zs)
                return True
            elif bi['value'] > last_zs['ZD']['value']:
                last_zs.update(ZD=bi)
            # 有可能成为离开段
            elif bi['value'] < last_zs['DD'][-1]['value']:
                last_zs['DD'].append(bi)
        else:
            raise ValueError
        last_zs['bi_list'].append(bi)

        return False

    def update_xd_eigenvalue(self, trade_date: list):
        xd = self.xd_list[-1]
        last_xd = self.xd_list[-2]
        xd.update(pct_change=(xd['value'] - last_xd['value']) / last_xd['value'])
        kn = trade_date.index(xd['date']) - trade_date.index(last_xd['date']) + 1
        xd.update(kn=kn)

    def update_xd(self, trade_date: list):
        """更新笔分型序列
        分型记对象样例：
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'd',
              'value': 138.0,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': >=1,
              'fx_power': 'weak'
          }

         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'g',
              'value': 150.67,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': 'down',
              'fx_power': 'strong'
          }
        """
        # 至少3根同类型分型才可能出现线段，最后1根bi不确定，因此最后一段也不确定
        if self.next is None:
            self.next = XdList()

        bi_list = self.xd_list
        xd_list = self.next

        if len(bi_list) < 4:
            return False

        if len(xd_list) < 1:
            # 线段不存在，初始化线段，找4个点的最高和最低点组成线段
            bi_list = bi_list[:-1].copy()
            bi_list = sorted(bi_list, key=lambda x: x['value'], reverse=False)
            if bi_list[0]['date'] < bi_list[-1]['date']:
                xd_list.append(bi_list[0])
                xd_list.append(bi_list[-1])
            else:
                xd_list.append(bi_list[-1])
                xd_list.append(bi_list[0])

            self.update_xd_eigenvalue(trade_date)
            return True

        bi3 = bi_list[-3]
        xd = bi_list[-1].copy()
        last_xd = xd_list[-1]
        xd2 = xd_list[-2]

        # 非分型结尾段，直接替换成分型, 没有新增段，后续不需要处理，同一个端点确认
        if 'fx_mark' not in last_xd or xd['date'] == last_xd['date']:
            xd_list[-1] = xd  # 日期相等的情况是否已经在内存中修改过了？
            self.update_xd_eigenvalue(trade_date)
            return True

        assert xd['date'] > last_xd['date']

        if bi3['fx_mark'] < 0:
            # 同向延续
            if last_xd['fx_mark'] < 0 and xd['value'] > last_xd['value']:
                xd_list[-1] = xd
                self.update_xd_eigenvalue(trade_date)
                return True
            # 反向判断
            elif last_xd['fx_mark'] > 0:
                # 价格判断
                if xd['value'] > xd2['value']:
                    xd_list.append(xd)
                    self.update_xd_eigenvalue(trade_date)
                    return True
                # 出现三笔破坏线段，连续两笔，一笔比一笔高,寻找段之间的最高点
                elif bi3['date'] > last_xd['date'] and xd['value'] > bi3['value']:
                    index = -5
                    bi = bi_list[index]
                    # 连续两个高点没有碰到段前面一个低点
                    try:
                        if bi['date'] < last_xd['date'] and \
                                bi_list[index - 1]['value'] > bi3['value'] and \
                                bi_list[index]['value'] > xd['value']:
                            return False
                    except Exception as err:
                        util_log_info('Last xd {}:{}'.format(last_xd['date'], err))

                    while bi['date'] > last_xd['date']:
                        if xd['value'] < bi['value']:
                            xd = bi
                        index = index - 2
                        bi = bi_list[index]
                    xd_list.append(xd)
                    self.update_xd_eigenvalue(trade_date)
                    return True
        elif bi3['fx_mark'] > 0:
            # 同向延续
            if last_xd['fx_mark'] > 0 and xd['value'] < last_xd['value']:
                xd_list[-1] = xd
                self.update_xd_eigenvalue(trade_date)
                return True
            # 反向判断
            elif last_xd['fx_mark'] < 0:
                # 价格判断
                if xd['value'] < xd2['value']:
                    xd_list.append(xd)
                    self.update_xd_eigenvalue(trade_date)
                    return True
                # 出现三笔破坏线段，连续两笔，一笔比一笔低,将最低的一笔作为段的起点，避免出现最低点不是端点的问题
                elif bi3['date'] > last_xd['date'] and xd['value'] < bi3['value']:
                    index = -5
                    bi = bi_list[index]
                    # 连续两个个低点没有碰到段前面一高低点
                    try:
                        if bi['date'] < last_xd['date'] and \
                                bi_list[index - 1]['value'] < bi3['value'] and \
                                bi_list[index]['value'] < xd['value']:
                            return False
                    except Exception as err:
                        util_log_info('Last xd {}:{}'.format(last_xd['date'], err))

                    while bi['date'] > last_xd['date']:
                        if xd['value'] > bi['value']:
                            xd = bi
                        index = index - 2
                        bi = bi_list[index]
                    xd_list.append(xd)
                    self.update_xd_eigenvalue(trade_date)
                    return True
        return False


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
          'direction': 1
      }

     {
         'date': Timestamp('2020-11-26 00:00:00'),
         'code': code,
          'fx_mark': 'g',
          'value': 150.67,
          'fx_start': Timestamp('2020-11-25 00:00:00'),
          'fx_end': Timestamp('2020-11-27 00:00:00'),
          'direction': -1
      }

      return: True 笔的数据出现更新，包括新增笔或者笔的延续
    """
    # 每根k线都要对bi进行判断
    if len(fx_list) < 2:
        return False

    bi = fx_list[-1].copy()

    # bi不需要考虑转折的分型强度
    # bi.pop('fx_power')
    # bi.update(code=self.code)  # 存储数据库需要
    # 没有笔时.最开始两个分型作为第一笔，增量更新时从数据库取出两个端点构成的笔时确定的
    if len(bi_list) < 1:
        bi2 = fx_list[-2].copy()
        # bi2.pop('fx_power')
        # bi2.update(code=self.code)
        bi_list.append(bi2)
        bi_list.append(bi)
        bi_list.update_xd_eigenvalue(trade_date)
        return False

    last_bi = bi_list[-1]

    bar = new_bars[-1].copy()
    bar.update(value=bar['high'] if bar['direction'] > 0 else bar['low'])

    # k 线确认模式，当前K线的日期比分型K线靠后，说明进来的数据时K线
    if bar['date'] > bi['fx_end']:
        if 'fx_mark' in last_bi:  # bi的结尾时分型
            # 趋势延续替代,首先确认是否延续
            if (last_bi['fx_mark'] < 0 and bar['high'] > last_bi['value']) \
                    or (last_bi['fx_mark'] > 0 and bar['low'] < last_bi['value']):
                bi_list[-1] = bar
                bi_list.update_xd_eigenvalue(trade_date)
                return True

            kn_inside = trade_date.index(bar['date']) - trade_date.index(last_bi['fx_end']) - 1

            # 必须被和笔方向相同趋势的k线代替，相反方向的会形成分型，由分型处理
            if kn_inside > 2 and bar['direction'] * last_bi['fx_mark'] > 0:  # 两个分型间至少有1根k线，端点有可能不是高低点
                bi_list.append(bar)
                bi_list.update_xd_eigenvalue(trade_date)
                return True

            # 只有一个端点，没有价格确认
            if len(bi_list) < 2:
                return False

            # 价格确认
            if (last_bi['fx_mark'] > 0 and bar['high'] > bi_list[-2]['value']) \
                    or (last_bi['fx_mark'] < 0 and bar['low'] < bi_list[-2]['value']):
                bi_list.append(bar)
                bi_list.update_xd_eigenvalue(trade_date)
                return True

        else:  # 原有未出现分型笔的延续，todo,只能替代原趋势，需要增加assert
            assert bar['direction'] * last_bi['direction'] > 0
            bi_list[-1] = bar
            bi_list.update_xd_eigenvalue(trade_date)
            return True

    # 非分型结尾笔，直接替换成分型, 没有新增笔，后续不需要处理，同一个端点确认
    if 'fx_mark' not in last_bi or bi['date'] == last_bi['date']:
        bi_list[-1] = bi
        bi_list.update_xd_eigenvalue(trade_date)
        return True

    # 分型处理，连续高低点处理，只判断是否后移,没有增加笔，不需要处理
    if last_bi['fx_mark'] * bi['fx_mark'] > 0:
        if (last_bi['fx_mark'] < 0 and last_bi['value'] < bi['value']) \
                or (last_bi['fx_mark'] > 0 and last_bi['value'] > bi['value']):
            bi_list[-1] = bi
            bi_list.update_xd_eigenvalue(trade_date)
            return True
    else:  # 笔确认是条件1、时间破坏，两个不同分型间至少有一根K线，2、价格破坏，向下的一笔破坏了上一笔的低点
        # 时间确认,函数算了首尾，所以要删除
        kn_inside = trade_date.index(bi['fx_start']) - trade_date.index(last_bi['fx_end']) - 1

        if kn_inside > 0:  # 两个分型间至少有1根k线，端点有可能不是高低点
            index = -2
            while fx_list[index]['date'] > last_bi['date']:

                # if bi['fx_mark'] == self._fx_list[index]['fx_mark']:
                #     if bi['fx_mark'] == 'd' and bi['value'] > self._fx_list[index]['value']:

                if (bi['fx_mark'] > 0 and 0 < fx_list[index]['fx_mark']
                    and bi['value'] > fx_list[index]['value']) \
                        or (bi['fx_mark'] < 0 and 0 > fx_list[index]['fx_mark']
                            and bi['value'] < fx_list[index]['value']):
                    bi = fx_list[index].copy()
                    # 分型结尾不变
                    bi['fx_end'] = fx_list[-1]['fx_end']

                index = index - 1

            bi_list.append(bi)
            bi_list.update_xd_eigenvalue(trade_date)
            return True

        # 只有一个端点，没有价格确认
        if len(bi_list) < 2:
            return False

        # 价格确认
        if (bi['fx_mark'] < 0 and bi['value'] > bi_list[-2]['value']) \
                or (bi['fx_mark'] > 0 and bi['value'] < bi_list[-2]['value']):
            bi_list.append(bi)
            bi_list.update_xd_eigenvalue(trade_date)
            return True


class CzscBase:
    def __init__(self, code, freq):
        self.freq = freq
        assert isinstance(code, str)
        self.code = code.upper()

        self._trade_date = []  # 用来查找索引
        self._bars = []
        self._new_bars = []
        self._fx_list = []
        self._xd_list = XdList()  # bi作为线段的head
        self._sig_list = []

    def update_sig(self):
        """
        缠论买卖信号，一二三买卖
        """
        if len(self._xd_list.zs_list) < 1:
            return False

        # 不确定性的笔来给出买卖信号
        zs = self._xd_list.zs_list[-1]
        bi = self._bi_list[-1]
        bar = self._bars[-1]
        last_new_bar = self._new_bars[-2]

        if bi['fx_mark'] == 'g':
            sig = {
                'bs': 'sell',
                'date': bar['date'],
                'value': last_new_bar['low']
            }
            # 中枢只有一根确定的笔，说明不是一卖就是二卖，小级别查趋势确认
            if len(zs['bi_list']) < 3:
                if bi['value'] > zs['ZG']['value']:
                    sig.update(type_="I_sell")
                else:
                    sig.update(type_="II_sell")
            # 三卖
            if bi['value'] < zs['ZD']['value']:
                sig.update(type_="III_sell")
            # 盘整背驰卖点,比较前一同向段,持续时间差不多的情况才有比较的价值
            elif bi['value'] > zs['GG'][-1]['value']:
                sig.update(type_="pb_sell")
        elif bi['fx_mark'] == 'd':
            sig = {
                'bs': 'buy',
                'date': bar['date'],
                'value': last_new_bar['high']
            }
            # 中枢只有一根确定的笔，说明不是一买就是二买，小级别查趋势确认
            if len(zs['bi_list']) < 3:
                if bi['value'] < zs['ZD']['value']:
                    sig.update(type_="I_buy")
                else:
                    sig.update(type_="II_buy")
            # 三买
            if bi['value'] > zs['ZG']['value']:
                sig.update(type_="III_buy")
            elif bi['value'] < zs['DD'][-1]['value']:
                sig.update(type_="pb_buy")
        else:
            raise ValueError

        if 'type_' in sig:
            self._sig_list.append(sig)
            return True

        return False

    def update(self):
        # 有包含关系时，不可能有分型出现，不出现分型时才需要

        update_fx(bars=self._bars, new_bars=self._new_bars, fx_list=self._fx_list, trade_date=self._trade_date)

        if not update_bi(
                new_bars=self._new_bars, fx_list=self._fx_list, bi_list=self._xd_list, trade_date=self._trade_date
        ):
            return

        # 新增确定性的笔才处理段
        xd_list = self._xd_list
        result = True
        while result:
            xd_list.update_zs()

            result = xd_list.update_xd(trade_date=self._trade_date)

            xd_list = xd_list.next

    #  必须实现,每次输入一个行情数据，然后调用update看是否需要更新
    def on_bar(self, bar):
        """
        输入数据格式
        Index(['open', 'high', 'low', 'close', 'amount', 'volume', 'date', 'code'], dtype='object')
        'date' 未 timestamp  volume用来画图
        """
        raise NotImplementedError


class CzscMongo(CzscBase):
    def __init__(self, code='rbl8', freq='day', exchange=None):
        # 只处理一个品种
        super().__init__(code, freq)

        # self._bi_list = fetch_future_bi_day(self.code, limit=2, format='dict')
        self._bi_list = []
        self.old_count = len(self._bi_list)
        if len(self._bi_list) > 0:
            # self._fx_list = self._bi_list
            start = self._bi_list[-1]['fx_end']
        else:
            start = '1990-01-01'

        self.data = get_bar(code, start, freq=freq, exchange=exchange)
        # self.Data = get_bar(code, start, end='2016-9-28', freq=freq, exchange=exchange)

    def draw(self, chart_path=None):
        chart = kline_pro(
            kline=self._bars, fx=self._fx_list,
            bs=self._sig_list, xd=self._xd_list,
            # title=self.code, width='1440px', height='580px'
            title=self.code, width='2540px', height='850px'
        )

        if not chart_path:
            chart_path = '{}.html'.format(self.code)
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
        self._bars.append(bar)

        self.update()

    def run(self, start=None, end=None):

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


class ClConsumer(ClThread):
    """
    缠中说禅关于分型，笔，段，中枢的指标,实盘模式
    """

    def __init__(self, code='rbl8', freq='day'):
        """
        code 可以是 str 或者 list
        :param code:
        :param freq:
        """
        super().__init__()
        self.freq = freq
        if isinstance(code, str):
            self.code = code.upper()
        elif isinstance(code, list):
            logging.info('Only handle one code!')
            # self.code = [c.upper() for c in code]
        else:
            logging.info('Wrong code format!')

        # self._c_dict = dict(
        #     [(c, Subscriber(exchange='bar_{}_{}'.format(frq, c), routing_key=c)) for c in self.code]
        # )
        self.c_bar = Subscriber(exchange='bar_{}_{}'.format(freq, self.code), routing_key=code)
        # self.segments = DATABASE.czsz_day.find({'code': self.code})
        self._bars = []
        self._new_bars = []
        self._fx_list = []
        self._bi_list = []

    def init(self):
        """
        只永久储存bi，储存数据时，最后一个bi的端点要是确定不会改变的，可以作为fx和bi处理的起始点
        """
        fx = fetch_future_bi_day(self.code, limit=2, format='dict')
        if len(fx) <= 2:
            self._fx_list = fx[-1:]
            # 需要至少两个数据，对笔的价格破坏需要通过比较同类分型
            self._bi_list = fx
            # 不能从fx_end开始，fx_end本身也可能是分型
            start = fx[-1]['date']
            self._bars = fetch_future_day(self.code, start=start, format='dict')

    @staticmethod
    def identify_direction(v1, v2):
        if v1 > v2:  # 前面几根可能都是包含，这里直接初始赋值down
            direction = "up"
        else:
            direction = "down"
        return direction

    def update_new_bars(self):
        assert len(self._bars) > 0
        bar = self._bars[-1].copy()

        # 第1根K线没有方向,不需要任何处理
        if len(self._bars) < 2:
            self._new_bars.append(bar)
            return False

        last_bar = self._new_bars[-1]

        cur_h, cur_l = bar['high'], bar['low']
        last_h, last_l, last_dt = last_bar['high'], last_bar['low'], last_bar['date']

        # 处理过包含关系，只需要用一个值识别趋势
        direction = self.identify_direction(cur_h, last_h)

        # 第2根K线只需要更新方向
        if len(self._bars) < 3:
            bar.update(direction=direction)
            self._new_bars.append(bar)
            return False

        # 没有包含关系，需要进行分型识别，趋势有可能改变
        if (cur_h > last_h and cur_l > last_l) or (cur_h < last_h and cur_l < last_l):
            bar.update(direction=direction)
            self._new_bars.append(bar)
            return True

        last_direction = last_bar.get('direction')

        # 有包含关系，不需要进行分型识别，趋势不改变
        # if (cur_h <= last_h and cur_l >= last_l) or (cur_h >= last_h and cur_l <= last_l):
        self._new_bars.pop(-1)  # 有包含关系的前一根数据被删除，这里是个技巧,todo 但会导致实际的高低点消失,只能低级别取处理
        # 有包含关系，按方向分别处理,同时需要更新日期
        if last_direction == "up":
            if cur_h < last_h:
                bar.update(high=last_h, date=last_dt)
            if cur_l < last_l:
                bar.update(low=last_l)
        elif last_direction == "down":
            if cur_l > last_l:
                bar.update(low=last_l, date=last_dt)
            if cur_h > last_h:
                bar.update(high=last_h)
        else:
            logging.error('{} last_direction: {} is wrong'.format(last_dt, last_direction))
            raise ValueError

        # 和前一根K线方向一致
        bar.update(direction=last_direction)

        self._new_bars.append(bar)
        return False

    def update_fx(self):
        """更新分型序列
        分型记对象样例：
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'd',
              'value': 138.0,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': 'up',
              'fx_power': 'weak'
          }

         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'g',
              'value': 150.67,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': 'down',
              'fx_power': 'strong'
          }
        """
        # 至少3根k线才能确定分型
        assert len(self._new_bars) >= 3

        bar1, bar2, bar3 = self._new_bars[-3], self._new_bars[-2], self._new_bars[-1]
        bar1_mid = (bar1['high'] - bar1['low']) / 2 + bar1['low']

        if bar1['high'] < bar2['high'] > bar3['high']:
            fx = {
                "date": bar2['date'],
                "fx_mark": "g",
                "value": bar2['high'],
                "fx_start": bar1['date'],  # 记录分型的开始和结束时间
                "fx_end": bar3['date'],
                "direction": bar3['direction'],
                'fx_power': 'strong' if bar3['close'] < bar1_mid else 'weak'
            }
            self._fx_list.append(fx)
            return True

        elif bar1['low'] > bar2['low'] < bar3['low']:
            fx = {
                "date": bar2['date'],
                "fx_mark": "d",
                "value": bar2['low'],
                "fx_start": bar1['date'],
                "fx_end": bar3['date'],
                "direction": bar3['direction'],
                'fx_power': 'strong' if bar3['close'] > bar1_mid else 'weak',
            }
            self._fx_list.append(fx)
            return True

        else:
            return False

    def update_bi(self):
        """更新笔序列
        笔标记对象样例：和分型标记序列结构一样
         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'd',
              'value': 138.0,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': 'up',
              'fx_power': 'weak'
          }

         {
             'date': Timestamp('2020-11-26 00:00:00'),
              'fx_mark': 'g',
              'value': 150.67,
              'fx_start': Timestamp('2020-11-25 00:00:00'),
              'fx_end': Timestamp('2020-11-27 00:00:00'),
              'direction': 'down',
              'fx_power': 'strong'
          }
        """
        bi = self._fx_list[-1]
        # print(bi)

        # 没有笔时.最开始两个分型作为第一笔，增量更新时从数据库取出两个端点构成的笔时确定的
        if len(self._bi_list) < 2:
            self._bi_list.append(bi)
            return False

        last_bi = self._bi_list[-1]

        # 连续高低点处理，只判断是否后移,没有增加笔，不需要处理
        if last_bi['fx_mark'] == bi['fx_mark']:
            if (last_bi['fx_mark'] == 'g' and last_bi['value'] < bi['value']) \
                    or (last_bi['fx_mark'] == 'd' and last_bi['value'] > bi['value']):
                self._bi_list[-1] = bi
                return False
        else:  # 笔确认是条件1、时间破坏，两个不同分型间至少有一根K线，2、价格破坏，向下的一笔破坏了上一笔的低点
            # 计算分型之间k线的根数
            # 价格确认，只有一笔时不做处理

            # 时间确认,函数算了首尾，所以要删除
            kn_inside = util_get_trade_gap(last_bi['fx_end'], bi['fx_start']) - 2

            if kn_inside > 0:  # 两个分型间至少有1根k线，端点有可能不是高低点
                self._bi_list.append(bi)
                return True

            # 只有一个端点，没有价格确认
            if len(self._bi_list) < 2:
                return False

            # 价格确认
            if (bi['fx_mark'] == 'g' and bi['value'] > self._bi_list[-2]['value']) \
                    or (bi['fx_mark'] == 'd' and bi['value'] < self._bi_list[-2]['value']):
                self._bi_list.append(bi)
                return True

    def update(self, bar):
        bar['dt'] = pd.to_datetime(bar['date'])
        if 'trade' in bar:
            bar['vol'] = bar.pop('trade')
        self._bars.append(bar)

        if not self.update_new_bars():
            return

        if not self.update_fx():
            return

        # 至少两个分型才能形成笔
        if not self.update_bi():
            return

    def on_bar(self, a, b, c, body):
        """
        行情数据进来，
        1、接收新的bar行情数据后调用，暂时只考虑bar结束时的调用
        2、检查是否存在分型数据，存在分型数据从最后一个分型数据开始处理
                            不存在分型数据，要从第二根k线开始处理，两根k线才能确定趋势
        3、
        """
        # 接收到终止消息后退出订阅
        if body == b'TERMINATED':
            self.c_bar.stop()
            # 储存数据
            pd.DataFrame(self._bi_list).to_csv('{}.csv'.format(self.code))
            return
        # print(body)
        bar = json.loads(body)
        # if pd.to_datetime(bar['date']) == pd.to_datetime('2020-10-14 00:00:00'):
        #     print('error')
        # todo check bar 是新的bar或者只是更新的bar，目前仅仅处理更新的bar，更新的bar要check周期上是否合理连续

        self.update(bar)

    def get_bar(self, format='dict'):
        if format in ['dict']:
            return self._bars
        elif format in ['pandas', 'p', 'P']:
            return pd.DataFrame(self._bars)

    def get_fx(self, format='dict'):
        if format in ['dict']:
            return self._fx_list
        elif format in ['pandas', 'p', 'P']:
            return pd.DataFrame(self._fx_list)

    def get_bi(self, format='dict'):
        if format in ['dict']:
            return self._bi_list
        elif format in ['pandas', 'p', 'P']:
            return pd.DataFrame(self._bi_list)

    def save_bi(self, collection=FACTOR_DATABASE.future_bi_day):
        # 先只考虑日线

        collection.create_index(
            [("code",
              pymongo.ASCENDING),
             ("date_stamp",
              pymongo.ASCENDING)]
        )

        # 首选查找数据库 是否有这个代码的数据,只需要返回日期
        ref = collection.find(
            {'code': self.code, 'type': {'$exists': True}},
            {'datetime': 1}
        )

        if ref.count() > 0:
            end = ref[ref.count() - 1]['datetime']
            start = util_get_next_day(end)
            if start - end <= pd.Timedelta(days=0):
                logging.info("Date is conflict with database!")
                return

        collection.insert_many(json.dumps(self.segments))

    def run(self):
        # for key in self._c_dict:
        #     self._c_dict[key].callback = self.on_bar
        #     self._c_dict[key].start()
        self.c_bar.callback = self.on_bar
        self.c_bar.start()
        print('Consumer terminated!')


def main_consumer():
    """
    测试使用
    :return:
    """
    # sim_bar = ClSimBar('rbl8', freq='day', start='2020-10-01')
    sim_bar = ClSimBar('rbl8', freq='day')
    czsc = ClConsumer('rbl8', freq='day')

    sim_bar.start()
    czsc.start()
    sim_bar.join(6000)
    czsc.join(6000)
    chart = kline_pro(kline=czsc.bar, fx=czsc.get_fx, bi=czsc.bi)
    # czsc.get_fx(format='p').to_csv('{}.csv'.format(czsc.code))
    chart_path = '{}.html'.format(czsc.code)
    chart.render(chart_path)
    # tab = Tab()
    # tab.add(chart, "day")
    # tab.render(chart_path)
    webbrowser.open(chart_path)


def main_mongo():
    czsc_mongo = CzscMongo(code='rul8', freq='day', exchange='dce')
    czsc_mongo.run()
    czsc_mongo.draw()


if __name__ == '__main__':
    # main_consumer()
    main_mongo()
