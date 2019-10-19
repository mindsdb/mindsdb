import logging
import json
import sys
import uuid
import os
import shutil
import importlib
import datetime

from colorlog import ColoredFormatter
import MySQLdb
import requests
import mindsdb
import lightwood


def setup_logger():
    formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(log_color)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'white',
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


def main():
    logger = setup_logger()
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'rb') as fp:
            cfg = json.load(fp)
    else:
        with open('config.json', 'rb') as fp:
            cfg = json.load(fp)
    con = MySQLdb.connect(cfg['mysql']['host'], cfg['mysql']['user'], cfg['mysql']['password'], cfg['mysql']['database'])
    cur = con.cursor()

    cur.execute("""CREATE DATABASE IF NOT EXISTS mindsdb_accuracy""")
    cur.execute("""CREATE TABLE IF NOT EXISTS mindsdb_accuracy.tests (
        batch_id                Text
        ,batch_started          Datetime
        ,test_name              Text
        ,dataset_name           Text
        ,accuracy               Float
        ,accuracy_function         Text
        ,accuracy_description   Text
        ,runtime                BIGINT
        ,started                Datetime
        ,ended                  Datetime
        ,mindsdb_version        Text
        ,lightwood_version      Text
    ) ENGINE=InnoDB""")

    batch_id = uuid.uuid4().hex
    batch_started = datetime.datetime.now()

    try:
        os.mkdir('tmp_downloads')
    except:
        pass

    TESTS = ['default_of_credit']

    test_data_arr = []
    for test_name in TESTS:
        '''
            @TODO: (async)
            Launch ec2 GPU machine
            Rsync this script onto it
            Execute the script there
            Download accuracy data and add it to the test_data_arr
            (await) Shut down machine
        '''
        r = requests.get(f'https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/{test_name}.tar.gz')
        with open(f'tmp_downloads/{test_name}.tar.gz', 'wb') as fp:
            fp.write(r.content)
        os.system(f'cd tmp_downloads && tar -xvf {test_name}.tar.gz && cd ..')
        os.remove(f'tmp_downloads/{test_name}.tar.gz')

        os.chdir(f'tmp_downloads/{test_name}')
        run_test = importlib.import_module(f'tmp_downloads.{test_name}.mindsdb_acc').run

        started = datetime.datetime.now()
        accuracy_data = run_test(False)
        ended = datetime.datetime.now()

        os.chdir('../..')

        accuracy = accuracy_data['accuracy']
        accuracy_function = accuracy_data['accuracy_function'] if 'accuracy_function' in accuracy_data else 'accuracy_score'
        accuracy_description = accuracy_data['accuracy_description'] if 'accuracy_description' in accuracy_data else ''

        test_data_arr.append({
            'test_name': test_name
            ,'dataset_name': test_name
            ,'accuracy': accuracy
            ,'accuracy_function': accuracy_function
            ,'accuracy_description': accuracy_description
            ,'started': started
            ,'ended': ended
            ,'runtime': (ended - started).total_seconds()
        })

    for test_data in test_data_arr:
        cur.execute("""INSERT INTO mindsdb_accuracy.tests VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(
            batch_id, batch_started, test_data['test_name'], test_data['dataset_name'],test_data['accuracy'],
            test_data['accuracy_function'], test_data['accuracy_description'],test_data['runtime'],
            test_data['started'], test_data['ended'], mindsdb.__version__, lightwood.__version__
        ))
        con.commit()

    shutil.rmtree('tmp_downloads')

    con.commit()
    con.close()

main()
