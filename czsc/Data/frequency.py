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

"""
时间频率转换为标准格式
"""
import re

from czsc.Utils import util_log_info


def parse_frequency_str(freq: str):
    """
    * 'Y', 'y', 'year'
    * 'Q', 'q', 'quarter'
    * 'M', 'month'
    * 'W', 'w', 'weeks', 'week'
    * 'D', 'd', 'days', 'day'
    * 'H', 'hours', 'hour', 'hr', 'h'
    * 'm', 'minute', 'min', 'minutes', 'T'
    * 'S', 'seconds', 'sec', 'second'
    * 'ms', 'milliseconds', 'millisecond', 'milli', 'millis', 'L'
    * 'us', 'microseconds', 'microsecond', 'micro', 'micros', 'U'
    * 'ns', 'nanoseconds', 'nano', 'nanos', 'nanosecond', 'N'
    """
    pattern = "^(?P<number>\d*)(?P<unit>[a-zA-z]+)"
    try:
        freq_dict = re.match(pattern, freq).groupdict()
    except:
        util_log_info('Wrong frequency format: {}'.format(freq))
        raise ValueError

    number = freq_dict['number']
    unit = freq_dict['unit']

    if unit in ['Y', 'y', 'year']:
        return 'Y'
    elif unit in ['Q', 'q', 'quarter']:
        return 'Q'
    elif unit in ['M', 'month']:
        return 'M'
    elif unit in ['W', 'w', 'weeks', 'week']:
        return 'w'
    elif unit in ['D', 'd', 'days', 'day']:
        return 'D'
    elif unit in ['H', 'hours', 'hour', 'hr', 'h']:
        if number:
            return str(number*60)+'min'
        else:
            return '60min'
    elif unit in ['m', 'minute', 'min', 'minutes', 'T']:
        return number + 'min'
    else:
        util_log_info('Wrong frequency format: {}'.format(freq))
        raise ValueError

