

import pika
from czsc.ClPubSub.setting import pubsub_ip, pubsub_port, pubsub_user, pubsub_password


class BasePubSub:
    # 只连接队列服务器
    def __init__(self, host=pubsub_ip, port=pubsub_port, user=pubsub_user, password=pubsub_password,
                 channel_number=1, queue_name='', routing_key='default',
                 exchange='', exchange_type='fanout', vhost='/'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self.queue_name = queue_name
        self.exchange = exchange
        self.routing_key = routing_key
        self.vhost = vhost
        self.exchange_type = exchange_type
        self.channel_number = channel_number

        credentials = pika.PlainCredentials(
            self.user, self.password, erase_on_connect=True)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, virtual_host=self.vhost,
                                      credentials=credentials, heartbeat=0, socket_timeout=5,
                                      )
        )

        self.channel = self.connection.channel(
            channel_number=self.channel_number)

    def reconnect(self):
        try:
            self.connection.close()
        except:
            pass

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port,
                                      heartbeat=0, virtual_host=self.vhost,
                                      socket_timeout=5,))

        self.channel = self.connection.channel(
            channel_number=self.channel_number)
        return self
