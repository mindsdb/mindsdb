import argparse

from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities.config import Config

def start(config):
    config = Config(config)
    MysqlProxy.startProxy(config)

if __name__ == '__main__':
    start()
