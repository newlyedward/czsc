# coding:utf-8

import os

"""创建本地文件夹
1. setting_path ==> 用于存放配置文件 setting.cfg
2. cache_path ==> 用于存放临时文件
3. log_path ==> 用于存放储存的log
"""

path = os.path.expanduser('~')
quanta_path = '{}{}{}'.format(path, os.sep, '.quanta')


def generate_path(name):
    return '{}{}{}'.format(quanta_path, os.sep, name)


def make_dir(path_name, exist_ok=True):
    os.makedirs(path_name, exist_ok=exist_ok)


setting_path = generate_path('setting')
cache_path = generate_path('cache')
log_path = generate_path('log')

make_dir(quanta_path, exist_ok=True)
make_dir(setting_path, exist_ok=True)
make_dir(cache_path, exist_ok=True)
make_dir(log_path, exist_ok=True)
