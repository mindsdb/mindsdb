import logging
import json

from colorlog import ColoredFormatter
import MySQLdb


def setup_logger():
    formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(log_color)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red',
        }
    )

    logger = logging.getLogger('test-logger')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def get_mysql(cfg_file):
    with open(cfg_file, 'rb') as fp:
        cfg = json.load(fp)

    con = MySQLdb.connect(cfg['mysql']['host'], cfg['mysql']['user'], cfg['mysql']['password'], cfg['mysql']['database'])
    cur = con.cursor()
    return con, cur, cfg
