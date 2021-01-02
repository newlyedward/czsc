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
import datetime
import logging
import webbrowser

from abc import ABCMeta, abstractmethod

import pymongo
from pyecharts.charts import Tab

from czsc.ClPubSub.consumer import Subscriber
from czsc.ClPubSub.producer import Publisher

import pandas as pd

from czsc.ClData.mongo import fetch_future_day, FACTOR_DATABASE, fetch_future_bi_day
from czsc.ClEngine.ClThread import ClThread
from czsc.ClUtils import kline_pro
from czsc.ClUtils.ClTradeDate import util_get_next_day, util_get_trade_gap, util_format_date2str


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

        self.frq = freq

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


class CzscBase:
    def __init__(self, code, freq):
        self.freq = freq
        if isinstance(code, str):
            self.code = [code.upper()]
        elif isinstance(code, list):
            self.code = [c.upper() for c in code]
        else:
            logging.info('Wrong code format!')

        self._bars = []
        self._new_bars = []
        self._fx_list = []
        self._bi_list = []

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
        if not self.update_new_bars():
            return

        if not self.update_fx():
            return

        # 至少两个分型才能形成笔
        if not self.update_bi():
            return

    #  必须实现,每次输入一个行情数据，然后调用update看是否需要更新
    def on_bar(self, bar):
        raise NotImplementedError


class CzscMongo(CzscBase):
    def __init__(self, code='rbl8', freq='day'):
        # 只处理一个品种
        super().__init__(code, freq)

    def run(self):
        data = fetch_future_day(self.code, self._start, self._end)
        data.apply(self.update, axis=1)

    def draw(self, chart_path=None):
        chart = kline_pro(kline=self._bars, fx=self._fx_list, bi=self._bi_list)
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
        if 'trade' in bar:
            bar['vol'] = bar.pop('trade')
        # bar['date'] = pd.to_datetime(bar['date'])
        self._bars.append(bar)

        self.update(bar)

    def run(self, start=None, end=None):
        start = '1990-01-01' if start is None else start
        end = str(datetime.date.today()) if end is None else end
        start = str(start)[0:10]
        end = str(end)[0:10]

        code = self.code[0]

        data = fetch_future_day(code, start, end)
        data.apply(self.on_bar, axis=1)


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

    # tab = Tab()
    # tab.add(chart, "day")
    # tab.render('{}.html'.format(czsc.code))
    # czsz.get_fx.to_csv('segments.csv')


def QA_indicator_SEGMENT(DataFrame, *args, **kwargs):
    """MA_VOLU

    Arguments:
        DataFrame {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    segments = []
    for index, row in DataFrame.reset_index().iterrows():
        try:
            segment = {
                'datetime': row['date'],
                'code': row['code'],
                'high': row['high'],
                'low': row['low'],
            }
        except:
            segment = {
                'datetime': row['datetime'],
                'code': row['code'],
                'high': row['high'],
                'low': row['low'],
            }

        idx = len(segments)

        if idx == 0:
            segments.append(segment)
            continue

        pre_segment = segments[-1]

        # 如果由于处理包含关系产生的高低点不存在的话，直接去前一根K线的高低点
        real_high_index = pre_segment.get('real_high_index', index - 1)
        real_low_index = pre_segment.get('real_low_index', index - 1)

        pre_high = segments[real_high_index]['high']
        pre_low = segments[real_low_index]['low']
        pre_trend = pre_segment.get('trend')

        trend = identify_trend(segment['high'], segment['low'], pre_high, pre_low)
        if trend in ['up', 'down']:
            segment.update(trend=trend)

        if trend not in ['up', 'down']:
            segment.update(trend=pre_trend)

        if pre_trend is None:
            # 初始存在包含关系，用最短的那根k线作为基准，起点的高低点会有误差
            if trend == 'include':
                segment.update(real_high_index=real_high_index, real_low_index=real_low_index)
            # 出现趋势就可以确认顶底
            elif trend == 'up':
                segment.update(type='bottom')
            elif trend == 'down':
                segment.update(type='peak')
            else:
                pass
        elif pre_trend == 'up':
            if trend == 'down':
                segment.update(type='peak')
                segment.update(real_high_index=pre_segment.get('real_high_index', - 1))
            elif trend == 'include':
                segment.update(real_low_index=pre_segment.get('real_low_index', - 1) - 1)
                # todo 需要判断高点是否比前一笔的高点高，如果高，新的笔形成
            elif trend == 'included':
                segment.update(real_high_index=pre_segment.get('real_high_index', - 1) - 1)
            else:
                pass
        elif pre_trend == 'down':
            if trend == 'up':
                segment.update(type='bottom')
                segment.update(real_low_index=pre_segment.get('real_low_index', - 1))
            elif trend == 'include':
                segment.update(real_high_index=pre_segment.get('real_high_index', - 1) - 1)
                # todo 需要判断高点是否比前一笔的高点高，如果高，新的笔形成
            elif trend == 'included':
                segment.update(real_low_index=pre_segment.get('real_low_index', - 1) - 1)
            else:
                pass
        else:
            pass

        segments.append(segment)

    return pd.DataFrame(segments)


def main_segment():
    """
    测试使用
    :return:
    """
    code = 'RBL8'
    start = '2020-01-01'
    # bar = QA_quotation(code, start, end=None, source=DATASOURCE.MONGO,
    #                    freq=FREQUENCE.DAY, market=MARKET_TYPE.FUTURE_CN,
    #                    output=OUTPUT_FORMAT.DATAFRAME)
    bar = fetch_future_day(code, start)

    segments = QA_indicator_SEGMENT(bar)
    segments.to_csv('segments.csv')


def main_mongo():
    czsc_mongo = CzscMongo(code='rbl8', freq='day')
    czsc_mongo.run()
    czsc_mongo.draw()


if __name__ == '__main__':
    # main_consumer()
    main_mongo()