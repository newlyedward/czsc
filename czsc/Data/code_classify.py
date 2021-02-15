# coding:utf-8
SSE_CLASSIFY_DICT = {
    '0': {
        '00': 'index',
        '09': 'T-bond',   # 2000年前发行
        '10': 'T-bond',   # 2000年-2009年发行
        '19': 'T-bond',   # 2010年及以后发行
    },
    '1': {
        '10': 'convertible',
        '11': 'convertible',
        '13': 'convertible',
        '18': 'convertible',
        '20': 'bond',
        '22': 'bond',
        '24': 'bond',
        '27': 'bond',
        '52': 'bond',
        '55': 'bond',
    },
    '2': {
        '04': 'repos',
    },
    '5': {
        '00': 'CEF',           # close-end fund 契约型行封闭式基金
        '01': 'LOEF',           # listed open-end fund  上市开放式基金
        '02': 'LOEF',
        '05': 'CEF',           # 创新型封闭式基金
        '06': 'LOF',           # list of fund 科创板LOF
        '10': 'ETF',           # 标的为沪市指数
        '12': 'ETF',           # 标的为跨市场指数
        '13': 'ETF',           # 标的为跨境指数
        '15': 'ETF',           # 标的为跨市场指数
        '16': 'ETF',           # 标的为跨市场指数
        '17': 'ETF',           # 标的为跨市场指数  517999-517499 （跨沪港深）
        '11': 'BETF',          # 交易型债券，货币基金
        '18': 'FETF',          # 交易型商品基金
    },
    '6': {
        '00': 'stock',
        '01': 'stock',
        '03': 'stock',
        '05': 'stock',
        '88': 'stock',         # 科创板股票
        '89': 'stock',         # 科创板存托凭证
    },
    '8': {
        '80': 'index',         # tdx统计指数
    },
    '9': {
        '00': 'B stock',
    },
}

SZSE_CLASSIFY_DICT = {
    '0': {
        '0': {
            '0': 'stock',
            '1': 'stock',
            '2': 'stock',
            '3': 'stock',
            '4': 'stock',
        },
    },
    '1': {
        '1': {
            '2': 'bond',            # 公司债
        },
        '2': {
            '0': 'EB',              # 可交换债
            '3': 'convertible',     # 创业板
            '7': 'convertible',     # 主板板
            '8': 'convertible',     # 中小板
        },
        '3': {
            '1': 'repos',           # 债券回购
        },
        '5': {
            '9': 'ETF'
        },
        '6': 'OEF',
    },
    '2': {
        '0': 'B stock',
    },
    '3': {
        '0': 'stock',
        '9': {
            '9': 'index',
        },
    },
}


def sse_code_classify(code):
    assert isinstance(code, str)

    first = code[0]
    second_third = code[1:3]

    try:
        return SSE_CLASSIFY_DICT[first][second_third]
    except:
        return 'Unkown'


def szse_code_classify(code):
    assert isinstance(code, str)

    first = code[0]
    second = code[1]
    third = code[2]

    try:
        type_ = SZSE_CLASSIFY_DICT[first][second]
        if isinstance(type_, dict):
            return type_[third]
        elif isinstance(type_, str):
            return type_
        else:
            return 'Unknown'
    except:
        return 'Unknown'



