# coding:utf-8

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

import threading
from queue import Queue

from czsc.ClUtils.ClRandom import util_random_with_topic

"""标准化的事件分发,可以快速引入和复用
每个事件需要携带一个方法,并且是需要立即被执行的时间才能使用这个事件方法"""


class ClThread(threading.Thread):
    """
    这是一个随意新建线程的生产者消费者模型'
    其实有个队列， 队列中保存的是 QA_Task 对象 ， callback 很重要，指定任务的时候可以绑定 函数执行
    QA_Engine 继承这个类。

    自带一个Queue
    有 self.put/ self.put_nowait/ self.get/ self.get_nowait 4个关于queue的方法

    如果你重写了run方法:
    则你需要自行处理queue中的事情/简单的做你自己的逻辑

    """

    def __init__(self, queue=None, name=None, daemon=False):
        threading.Thread.__init__(self)
        self.queue = Queue() if queue is None else queue
        self.thread_stop = False
        self.__flag = threading.Event()  # 用于暂停线程的标识
        self.__flag.set()  # 设置为True
        self.__running = threading.Event()  # 用于停止线程的标识
        self.__running.set()  # 将running设置为True
        self.name = util_random_with_topic(
            topic='czsc',
            lens=3
        ) if name is None else name
        self.idle = False
        self.daemon = daemon

    def __repr__(self):
        return '<QA_Thread: {}  id={} ident {}>'.format(
            self.name,
            id(self),
            self.ident
        )

    def run(self):
        while self.__running.isSet():
            self.__flag.wait()
            while not self.thread_stop:
                '这是一个阻塞的队列,避免出现消息的遗漏'
                try:
                    if self.queue.empty() is False:
                        _task = self.queue.get()  # 接收消息
                        # print(_task.worker, self.name)
                        assert isinstance(_task, Cl_Task)
                        if _task.worker != None:

                            _task.do()

                            self.queue.task_done()  # 完成一个任务
                        else:
                            pass
                    else:
                        self.idle = True

                        # Mac book下风扇狂转，如果sleep cpu 占用率回下降
                        # time.sleep(0.01)
                except Exception as e:
                    if isinstance(e, ValueError):
                        pass
                    else:
                        raise e

    def pause(self):
        self.__flag.clear()

    def resume(self):
        self.__flag.set()  # 设置为True, 让线程停止阻塞

    def stop(self):
        # self.__flag.set()       # 将线程从暂停状态恢复, 如何已经暂停的话
        self.__running.clear()
        self.thread_stop = True  # 设置为False

    def __start(self):
        self.queue.start()

    def put(self, task):
        self.queue.put(task)

    def put_nowait(self, task):
        self.queue.put_nowait(task)

    def get(self):
        return self.queue.get()

    def get_nowait(self):
        return self.queue.get_nowait()

    def qsize(self):
        return self.queue.qsize()


if __name__ == '__main__':
    import queue

    q = queue.Queue()
