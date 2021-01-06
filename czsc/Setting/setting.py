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

import configparser
import json
import os
from multiprocessing import Lock

import pymongo

from czsc.Setting.localize import quanta_path, setting_path

# 配置目录存放在 ~/.quanta
# 如果配置目录不存在就创建，主要配置都保存在config.ini里面

DEFAULT_MONGO = os.getenv('MONGODB', 'localhost')
DEFAULT_DB_URI = 'mongodb://{}:27017'.format(DEFAULT_MONGO)
DEFAULT_TDX_PATH = os.path.expanduser('D:\\Trade\\TDX\\QHT')
CONFIGFILE_PATH = '{}{}{}'.format(setting_path, os.sep, 'config.ini')


def mongo_uri_setting(uri='mongodb://localhost:27017/quanta'):
    """
    uri=mongodb://user:passwor@ip:port
    """
    client = pymongo.MongoClient(uri)
    return client


class Setting:

    def __init__(self, uri=None):
        self.lock = Lock()

        self.mongo_uri = uri or self.get_mongo_uri()
        self.username = None
        self.password = None
        self.config = configparser.ConfigParser()
        if os.path.exists(CONFIGFILE_PATH):
            self.config.read(CONFIGFILE_PATH)
        else:
            f = open('{}{}{}'.format(setting_path, os.sep, 'config.ini'), 'w')
            # 写入默认数据库地址
            self.config.add_section('MONGODB')
            self.config.set('MONGODB', 'uri', DEFAULT_DB_URI)
            # 写入默认 TDX 路径
            self.config.add_section('TDX')
            self.config.set('TDX', 'root', DEFAULT_TDX_PATH)

            self.config.write(f)

    def get_mongo_uri(self):
        """

        """
        try:
            res = self.config.get('MONGODB', 'uri')
        except:
            res = DEFAULT_DB_URI
        return res

    def get_config(
            self,
            section='MONGODB',
            option='uri',
            default_value=DEFAULT_DB_URI
    ):
        try:
            return self.config.get(section, option)
        except:  # 数据库也存有setting
            res = self.client.quanta.usersetting.find_one(
                {'section': section})
            if res:
                return res.get(option, default_value)
            else:
                self.set_config(section, option, default_value)
                return default_value

    def set_config(
            self,
            section='MONGODB',
            option='uri',
            default_value=DEFAULT_DB_URI
    ):

        t = {'section': section, option: default_value}
        self.client.quanta.usersetting.update(
            {'section': section}, {'$set': t}, upsert=True)

    @property
    def client(self):
        """
        uri=mongodb://user:passwor@ip:port
        """
        return pymongo.MongoClient(self.mongo_uri)

    @property
    def tdx_dir(self):
        """
        tdx根目录
        """
        return self.get_config(section='TDX', option='root', default_value=DEFAULT_TDX_PATH)


SETTING = Setting()
DATABASE = SETTING.client.quanta
TDX_DIR = SETTING.tdx_dir
