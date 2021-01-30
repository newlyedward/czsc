# coding: utf-8
from collections import OrderedDict


def find_zs(points):
    """
    输入笔或线段标记点，输出中枢识别结果
    {'dt': Timestamp('2020-11-26 00:00:00'),
          'fx_mark': 'd',
          'value': 138.0，
          'start_dt': Timestamp('2020-11-25 00:00:00'),
          'end_dt': Timestamp('2020-11-27 00:00:00'),
          'fx_high': 144.87, 往上延申最高点 避免出现笔的端点不是极值点的情况
          'fx_low': 138.0, 这个数据重复，可以用来记录笔的结束极值点的情况，一般情况根下一笔的}
    """
    if len(points) < 5:  # 4段才能形成中枢？，3段就有重合部分
        return []
    # 统一使用value字段，不区分段或者笔
    # # 当输入为笔的标记点时，新增 xd 值
    # for j, x in enumerate(points):
    #     if x.get("bi", 0):
    #         points[j]['xd'] = x["bi"]

    def __get_zn(zn_points_):
        """把与中枢方向一致的次级别走势类型称为Z走势段，按中枢中的时间顺序，
        分别记为Zn等，而相应的高、低点分别记为gn、dn"""
        if len(zn_points_) % 2 != 0: # 偶数点，奇数段，进入段和离开段保证方向一致？
            zn_points_ = zn_points_[:-1]

        # 根据进入段确认中枢的方向
        if zn_points_[0]['fx_mark'] == "d":
            z_direction = "up"
        else:
            z_direction = "down"

        zn = []
        for i in range(0, len(zn_points_), 2):
            zn_ = {
                "start_dt": zn_points_[i]['dt'],
                "end_dt": zn_points_[i + 1]['dt'],
                "high": max(zn_points_[i]['value'], zn_points_[i + 1]['value']),
                "low": min(zn_points_[i]['value'], zn_points_[i + 1]['value']),
                "direction": z_direction
            }
            zn_['mid'] = zn_['low'] + (zn_['high'] - zn_['low']) / 2
            zn.append(zn_)
        return zn

    k_xd = points
    k_zs = []
    zs_xd = []

    for i in range(len(k_xd)):
        if len(zs_xd) < 5:
            zs_xd.append(k_xd[i])
            continue
        xd_p = k_xd[i]
        # 计算中枢高低点 ，4个点三段
        zs_d = max([x['value'] for x in zs_xd[:4] if x['fx_mark'] == 'd'])
        zs_g = min([x['value'] for x in zs_xd[:4] if x['fx_mark'] == 'g'])
        if zs_g <= zs_d:  # 有重合属于同一个中枢
            zs_xd.append(xd_p)
            zs_xd.pop(0)
            continue

        # 定义四个指标,GG=max(gn),G=min(gn),D=max(dn),DD=min(dn)，n遍历中枢中所有Zn。
        # 定义ZG=min(g1、g2), ZD=max(d1、d2)，显然，[ZD，ZG]就是缠中说禅走势中枢的区间
        if xd_p['fx_mark'] == "d" and xd_p['value'] > zs_g:
            zn_points = zs_xd[3:]
            # 线段在中枢上方结束，形成三买
            k_zs.append({
                'ZD': zs_d,
                "ZG": zs_g,
                'G': min([x['xd'] for x in zs_xd if x['fx_mark'] == 'g']),
                'GG': max([x['xd'] for x in zs_xd if x['fx_mark'] == 'g']),
                'D': max([x['xd'] for x in zs_xd if x['fx_mark'] == 'd']),
                'DD': min([x['xd'] for x in zs_xd if x['fx_mark'] == 'd']),
                'start_point': zs_xd[1],
                'end_point': zs_xd[-2],
                "zn": __get_zn(zn_points),
                "points": zs_xd,
                "third_buy": xd_p
            })
            zs_xd = []
        elif xd_p['fx_mark'] == "g" and xd_p['xd'] < zs_d:
            zn_points = zs_xd[3:]
            # 线段在中枢下方结束，形成三卖
            k_zs.append({
                'ZD': zs_d,
                "ZG": zs_g,
                'G': min([x['xd'] for x in zs_xd if x['fx_mark'] == 'g']),
                'GG': max([x['xd'] for x in zs_xd if x['fx_mark'] == 'g']),
                'D': max([x['xd'] for x in zs_xd if x['fx_mark'] == 'd']),
                'DD': min([x['xd'] for x in zs_xd if x['fx_mark'] == 'd']),
                'start_point': zs_xd[1],
                'end_point': zs_xd[-2],
                "points": zs_xd,
                "zn": __get_zn(zn_points),
                "third_sell": xd_p
            })
            zs_xd = []
        else:
            zs_xd.append(xd_p)

    if len(zs_xd) >= 5:
        zs_d = max([x['xd'] for x in zs_xd[:4] if x['fx_mark'] == 'd'])
        zs_g = min([x['xd'] for x in zs_xd[:4] if x['fx_mark'] == 'g'])
        if zs_g > zs_d:
            zn_points = zs_xd[3:]
            k_zs.append({
                'ZD': zs_d,
                "ZG": zs_g,
                'G': min([x['xd'] for x in zs_xd if x['fx_mark'] == 'g']),
                'GG': max([x['xd'] for x in zs_xd if x['fx_mark'] == 'g']),
                'D': max([x['xd'] for x in zs_xd if x['fx_mark'] == 'd']),
                'DD': min([x['xd'] for x in zs_xd if x['fx_mark'] == 'd']),
                'start_point': zs_xd[1],
                'end_point': None,
                "zn": __get_zn(zn_points),
                "points": zs_xd,
            })
    return k_zs


def check_jing(fd1, fd2, fd3, fd4, fd5) -> str:
    """检查最近5个分段走势是否构成井

    井的主要用途和背驰是一样的，用来判断趋势的结束。用在盘整也可以，但效果稍差点。

    井的定义：
        12345，五段，是构造井的基本形态，形成井的位置肯定是5，而5出井的
        前提条件是对于向上5至少比3和1其中之一高，向下反过来; 并且，234
        构成一个中枢。

        井只有两类，大井和小井（以向上为例）：
        大井对应的形式是：12345向上，5最高3次之1最低，力度上1大于3，3大于5；
        小井对应的形式是：
            1：12345向上，3最高5次之1最低，力度上5的力度比1小，注意这时候
               不需要再考虑5和3的关系了，因为5比3低，所以不需要考虑力度。
            2：12345向上，5最高3次之1最低，力度上1大于5，5大于3。

        小井的构造，关键是满足5一定至少大于1、3中的一个。
        注意有一种情况不归为井：就是12345向上，1的力度最小，5的力度次之，3的力度最大此类不算井，
        因为345后面必然还有走势在67的时候才能再判断，个中道理各位好好体会。


    fd 为 dict 对象，表示一段走势，可以是笔、线段。

    假定最近一段走势为第N段；则 fd1 为第N-4段走势, fd2为第N-3段走势,
    fd3为第N-2段走势, fd4为第N-1段走势, fd5为第N段走势

    :param fd1: 第N-4段
    :param fd2: 第N-3段
    :param fd3: 第N-2段
    :param fd4: 第N-1段
    :param fd5: 第N段
    :return:
    """
    assert fd1['direction'] == fd3['direction'] == fd5['direction']
    assert fd2['direction'] == fd4['direction']
    direction = fd1['direction']

    zs_g = min(fd2['high'], fd3['high'], fd4['high'])
    zs_d = max(fd2['low'], fd3['low'], fd4['low'])

    jing = "other"

    if fd1['price_power'] < fd5['price_power'] < fd3['price_power'] \
            and fd1['vol_power'] < fd5['vol_power'] < fd3['vol_power']:
        # 1的力度最小，5的力度次之，3的力度最大，此类不算井
        return jing

    if zs_d < zs_g:  # 234有中枢的情况
        if direction == 'up' and fd5["high"] > min(fd3['high'], fd1['high']):
            # 大井: 12345向上，5最高3次之1最低，力度上1大于3，3大于5
            if fd5["high"] > fd3['high'] > fd1['high'] \
                    and fd5['price_power'] < fd3['price_power'] < fd1['price_power'] \
                    and fd5['vol_power'] < fd3['vol_power'] < fd1['vol_power']:
                jing = "向上大井"

            # 第一种小井: 12345向上，3最高5次之1最低，力度上5的力度比1小
            if fd1['high'] < fd5['high'] < fd3['high'] \
                    and fd5['price_power'] < fd1['price_power'] \
                    and fd5['vol_power'] < fd1['vol_power']:
                jing = "向上小井A"

            # 第二种小井: 12345向上，5最高3次之1最低，力度上1大于5，5大于3
            if fd5["high"] > fd3['high'] > fd1['high'] \
                    and fd1['price_power'] > fd5['price_power'] > fd3['price_power'] \
                    and fd1['vol_power'] > fd5['vol_power'] > fd3['vol_power']:
                jing = "向上小井B"

        if direction == 'down' and fd5["low"] < max(fd3['low'], fd1['low']):

            # 大井: 12345向下，5最低3次之1最高，力度上1大于3，3大于5
            if fd5['low'] < fd3['low'] < fd1['low'] \
                    and fd5['price_power'] < fd3['price_power'] < fd1['price_power'] \
                    and fd5['vol_power'] < fd3['vol_power'] < fd1['vol_power']:
                jing = "向下大井"

            # 第一种小井: 12345向下，3最低5次之1最高，力度上5的力度比1小
            if fd1["low"] > fd5['low'] > fd3['low'] \
                    and fd5['price_power'] < fd1['price_power'] \
                    and fd5['vol_power'] < fd1['vol_power']:
                jing = "向下小井A"

            # 第二种小井: 12345向下，5最低3次之1最高，力度上1大于5，5大于3
            if fd5['low'] < fd3['low'] < fd1['low'] \
                    and fd1['price_power'] > fd5['price_power'] > fd3['price_power'] \
                    and fd1['vol_power'] > fd5['vol_power'] > fd3['vol_power']:
                jing = "向下小井B"
    return jing


def check_third_bs(fd1, fd2, fd3, fd4, fd5) -> str:
    """输入5段走势，判断是否存在第三类买卖点

    :param fd1: 第N-4段
    :param fd2: 第N-3段
    :param fd3: 第N-2段
    :param fd4: 第N-1段
    :param fd5: 第N段
    :return:
    """
    zs_d = max(fd1['low'], fd2['low'], fd3['low'])
    zs_g = min(fd1['high'], fd2['high'], fd3['high'])

    third_bs = "other"

    if fd5['high'] < zs_d < zs_g and fd4['low'] < min(fd1['low'], fd3['low']):
        third_bs = "三卖"

    if fd5['low'] > zs_g > zs_d and fd4['high'] > max(fd1['high'], fd3['high']):
        third_bs = "三买"

    return third_bs


def check_dynamic(fd1, fd3, fd5):
    """计算第N段走势的涨跌力度

    向上笔不创新高，向上笔新高盘背，向上笔新高无背
    向下笔不创新低，向下笔新低盘背，向下笔新低无背

    :param fd1: 第N-4段走势
    :param fd3: 第N-2段走势
    :param fd5: 第N段走势
    :return: str
    """
    if fd5['direction'] == "up":
        if fd5['high'] < fd3['high'] or fd5['high'] < fd1['high']:
            v = "向上笔不创新高"
        else:
            if fd5['price_power'] > fd3['price_power'] and fd5['price_power'] > fd1['price_power'] \
                    and fd5['vol_power'] > fd3['vol_power'] and fd5['vol_power'] > fd1['vol_power']:
                v = "向上笔新高无背"
            else:
                v = "向上笔新高盘背"
    elif fd5['direction'] == "down":
        if fd5['low'] > fd3['low'] or fd5['low'] > fd1['low']:
            v = "向下笔不创新低"
        else:
            if fd5['price_power'] > fd3['price_power'] and fd5['price_power'] > fd1['price_power'] \
                    and fd5['vol_power'] > fd3['vol_power'] and fd5['vol_power'] > fd1['vol_power']:
                v = "向下笔新低无背"
            else:
                v = "向下笔新低盘背"
    else:
        raise ValueError
    return v


