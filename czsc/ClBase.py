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

import pymongo

from czsc.ClPubSub.consumer import Subscriber
from czsc.ClPubSub.producer import Publisher

import pandas as pd

from czsc.ClData.mongo import fetch_future_day, FACTOR_DATABASE, fetch_future_bi_day
from czsc.ClEngine.ClThread import ClThread
from czsc.ClUtils.ClTradeDate import util_get_next_day


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
        code = item.name[1]
        print('send {} {} quotation!'.format(item.name[0], code))
        producer = self.pro.get(code, Publisher(exchange='bar_{}_{}'.format(self.frq, code)))
        bar = item.to_dict()
        # todo 对输入字符进行模糊判断，转换为统一的频率标识,日期时间数据统一用datetime表示
        bar.update(code=code, datetime=item.name[0].strftime("%Y-%m-%d %H:%M:%S"))
        producer.pub(json.dumps(bar))
        # time.sleep(1)

    def run(self):
        # todo 从多个数据源取数据
        data = fetch_future_day(self.code, self._start, self._end)
        data.apply(self.publish_bar, axis=1)

        for code in self.code:
            producer = self.pro.get(code, Publisher(exchange='bar_{}_{}'.format(self.frq, code)))
            producer.pub('TERMINATED')


def identify_trend(h, l, pre_h, pre_l):
    if h > pre_h and l > pre_l:
        return 'up'
    elif h < pre_h and l < pre_l:
        return 'down'
    elif h >= pre_h and l <= pre_l:
        return 'include'
    elif h <= pre_h and l >= pre_l:
        return 'included'


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
        self.bars = []
        self.new_bars = []
        self.fx_list = []
        self.bi_list = []

    def init(self):
        """
        只永久储存bi，储存数据时，最后一个bi的端点要是确定不会改变的，可以作为fx和bi处理的起始点
        """
        fx = fetch_future_bi_day(self.code, limit=2, format='dict')
        if len(fx) <= 2:
            self.fx_list = fx[-1:]
            # 需要至少两个数据，对笔的价格破坏需要通过比较同类分型
            self.bi_list = fx
            # 不能从fx_end开始，fx_end本身也可能是分型
            start = fx[-1]['date']
            self.bars = fetch_future_day(self.code, start=start, format='dict')

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
            return
        bar = json.loads(body)
        # todo check bar 是新的bar或者只是更新的bar，目前仅仅处理更新的bar，更新的bar要check周期上是否合理连续
        self.bars.append(bar)

        # 2根k线才能确定方向
        if len(self.bars) < 3:
            self.new_bars.append(bar)

        last_bar = self.new_bars[-1]  # 前面处理过包含关系，只用高点判断趋势
        if last_bar['high'] > self.new_bars[-2]['high']:  # 前面几根可能都是包含，这里直接初始赋值down
            direction = "up"
        else:
            direction = "down"

        cur_h, cur_l = bar['high'], bar['low']
        last_h, last_l, last_dt = last_bar['high'], last_bar['low'], last_bar['date']
        if (cur_h <= last_h and cur_l >= last_l) or (cur_h >= last_h and cur_l <= last_l):
            self.new_bars.pop(-1)  # 有包含关系的前一根数据被删除，这里是个技巧,todo 但会导致实际的高低点消失,只能低级别取处理
            # 有包含关系，按方向分别处理,同时需要更新日期
            if direction == "up":
                if cur_h < last_h:
                    bar.update(high=last_h, dt=last_dt)
                if cur_l < last_l:
                    bar.update(low=last_l)
            elif direction == "down":
                if cur_l > last_l:
                    bar.update(low=last_l, dt=last_dt)
                if cur_h > last_h:
                    bar.update(high=last_h)
            else:
                raise ValueError

        self.new_bars.append(bar)

        # 至少3根k线才能确定分型
        if len(self.new_bars) < 3:
            return

        bar1, bar2, bar3 = self.new_bars[-2], self.new_bars[-1], bar
        bar1_mid = (bar1['high'] - bar1['low']) / 2 + bar1['low']
        if bar1['high'] < bar2['high'] > bar3['high']:
            fx = {
                "dt": bar2['dt'],
                "fx_mark": "g",
                "value": bar2['high'],
                "fx_start": bar1['dt'],  # 记录分型的开始和结束时间
                "fx_end": bar3['dt'],
                'fx_power': 'strong' if bar3['close'] < bar1_mid else 'weak',
                "fx_high": bar2['high'],
                "fx_low": bar1['low'],  # 顶分型，分型前一个k线的低点
            }
            is_new_fx = True

        elif bar1['low'] > bar2['low'] < bar3['low']:

            fx = {
                "dt": bar2['dt'],
                "fx_mark": "d",
                "value": bar2['low'],
                "fx_start": bar1['dt'],
                "fx_end": bar3['dt'],
                'fx_power': 'strong' if bar3['close'] > bar1_mid else 'weak',
                "fx_high": bar1['high'],
                "fx_low": bar2['low'],
            }
            is_new_fx = True

        if is_new_fx:
            fx.update(date=bar2['date'],
                "value": bar2['low'],
                "fx_start": bar1['dt'],
                "fx_end": bar3['dt'],
                'fx_power': 'strong' if bar3['close'] > bar1_mid else 'weak',
                "fx_high": bar1['high'],
                "fx_low": bar2['low'],
            }
            self.fx_list.append(fx)
        # 从上一个分型点开始处理

        # 数据处理只需要记录时间和高低点
        segment = {
            'datetime': bar['datetime'],
            'code': bar['code'],
            'high': bar['high'],
            'low': bar['low'],
        }

        index = len(self.segments)

        # todo 初始化操作， 避免都从第一个数据开始处理
        if index == 0:
            if len(self.segments) == 0:
                # 从数据库中取数据的最后一个记录
                self.segments = []

                # 如果没有数据记录，说明是第一根K线
            if len(self.segments) == 0:
                # pb.update(trend='include', pb_high=bar['high'], pb_low=bar['low'])
                self.segments.append(segment)
                return
            else:
                pass

        pre_segment = self.segments[-1]

        # 如果由于处理包含关系产生的高低点不存在的话，直接去前一根K线的高低点
        real_high_index = pre_segment.get('real_high_index', index - 1)
        real_low_index = pre_segment.get('real_low_index', index - 1)

        pre_high = self.segments[real_high_index]['high']
        pre_low = self.segments[real_low_index]['low']
        pre_trend = pre_segment.get('trend')

        trend = identify_trend(segment['high'], segment['low'], pre_high, pre_low)
        if trend in ['up', 'down']:
            segment.update(trend=trend)

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

        if pre_trend == 'up':
            if trend == 'down':
                segment.update(type='peak')
            elif trend == 'include':
                segment.update(real_low_index=pre_segment.get('real_low_index', - 1) - 1)
                # todo 需要判断高点是否比前一笔的高点高，如果高，新的笔形成
            elif trend == 'included':
                segment.update(real_high_index=pre_segment.get('real_high_index', - 1) - 1)
            else:
                pass

        if pre_trend == 'down':
            if trend == 'up':
                segment.update(type='bottom')
            elif trend == 'include':
                segment.update(real_high_index=pre_segment.get('real_high_index', - 1) - 1)
                # todo 需要判断高点是否比前一笔的高点高，如果高，新的笔形成
            elif trend == 'included':
                segment.update(real_low_index=pre_segment.get('real_low_index', - 1) - 1)
            else:
                pass

        self.segments.append(segment)
        if 'type' in segment:
            # todo 将分型插入数据库
            print('Publish {} peak and bottom message!'.format(segment['datetime']))
            result = FACTOR_DATABASE.future_bi_day.insert_one(segment)
            print(result.inserted_id)
        return

    @property
    def fenxing(self):
        return pd.DataFrame(self.segments)

    def save_segments(self, collection=FACTOR_DATABASE.future_bi_day):
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
    sim_bar = ClSimBar('rbl8', freq='day', start='2020-10-01')
    czsz = ClConsumer('rbl8', freq='day')

    sim_bar.start()
    czsz.start()
    sim_bar.join(6000)
    czsz.join(6000)
    czsz.fenxing.to_csv('segments.csv')


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


if __name__ == '__main__':
    main_consumer()
