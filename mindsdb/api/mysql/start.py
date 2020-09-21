import argparse

from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities.config import Config

def start(config, initial=False):
    if not initial:
        print('\n\nWarning, this process should not have been started... nothing is "wrong" but it needlessly ate away a tiny bit of precious compute !\n\n')
    config = Config(config)
    MysqlProxy.startProxy(config)

if __name__ == '__main__':
    start()
