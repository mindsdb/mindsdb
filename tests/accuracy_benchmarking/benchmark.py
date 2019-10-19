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
import ludwig


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


def run_benchmarks():
    logger = setup_logger()
    if len(sys.argv) > 2:
        with open(sys.argv[2], 'rb') as fp:
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
        ,ludwig_version         Text
        ,backend                Text
    ) ENGINE=InnoDB""")

    batch_id = uuid.uuid4().hex
    batch_started = datetime.datetime.now()

    try:
        os.mkdir('tmp_downloads')
    except:
        pass

    #TESTS = ['default_of_credit', 'cancer50', 'pulsar_stars', 'imdb_movie_review'] # cifar_100
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
        backend_used = accuracy_data['backend'] if 'backend' in accuracy_data else 'unknown'

        test_data_arr.append({
            'test_name': test_name
            ,'dataset_name': test_name
            ,'accuracy': accuracy
            ,'accuracy_function': accuracy_function
            ,'accuracy_description': accuracy_description
            ,'started': started
            ,'ended': ended
            ,'runtime': (ended - started).total_seconds()
            ,'backend_used': backend_used
        })

    for test_data in test_data_arr:
        cur.execute("""INSERT INTO mindsdb_accuracy.tests VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(
            batch_id, batch_started, test_data['test_name'], test_data['dataset_name'],test_data['accuracy'],
            test_data['accuracy_function'], test_data['accuracy_description'],test_data['runtime'], test_data['started'],
            test_data['ended'], mindsdb.__version__, lightwood.__version__, ludwig.__version__, test_data['backend_used']
        ))
        con.commit()

    shutil.rmtree('tmp_downloads')

    con.commit()
    con.close()


def compare():
    logger = setup_logger()
    if len(sys.argv) > 2:
        with open(sys.argv[2], 'rb') as fp:
            cfg = json.load(fp)
    else:
        with open('config.json', 'rb') as fp:
            cfg = json.load(fp)

    con = MySQLdb.connect(cfg['mysql']['host'], cfg['mysql']['user'], cfg['mysql']['password'], cfg['mysql']['database'])
    cur = con.cursor()

    if len(sys.argv) > 3:
        batch_id = sys.argv[3]
    # Go with the latest batch by default
    else:
        cur.execute('SELECT batch_id FROM mindsdb_accuracy.tests WHERE batch_started=(SELECT max(batch_started) FROM mindsdb_accuracy.tests)')
        batch_id = cur.fetchall()[0][0]

    cur.execute('SELECT test_name, accuracy_function, accuracy, runtime, mindsdb_version, lightwood_version, ludwig_version, backend FROM mindsdb_accuracy.tests WHERE batch_id=%s', [batch_id])
    batch_tests_arr = cur.fetchall()

    for entity in batch_tests_arr:
        test_name = entity[0]
        accuracy_function = entity[1]
        accuracy = entity[2]
        runtime = entity[3]

        mindsdb_version = entity[4]
        lightwood_version = entity[5]
        ludwig_version = entity[6]
        backend = entity[7]

        logger.info(f'\nRunning checks for test {test_name} in batch {batch_id}, the test took {runtime} seconds and had an accuracy of {accuracy} using {accuracy_function} and training on backend {backend}\nSystem info when ran: (mindsdb_version: {mindsdb_version}, lightwood_version: {lightwood_version}, ludwig_version: {ludwig_version})\n')

        cur.execute('SELECT accuracy, batch_id, batch_started, runtime, mindsdb_version, lightwood_version, ludwig_version, backend FROM mindsdb_accuracy.tests WHERE test_name=%s AND accuracy_function=%s AND accuracy > %s', [test_name,accuracy_function,accuracy])
        better_tests = cur.fetchall()

        for row in better_tests:
            old_accuracy = row[0]
            old_batch_id = row[1]
            old_batch_started = row[2]
            old_runtime = row[3]

            old_mindsdb_version = entity[4]
            old_lightwood_version = entity[5]
            old_ludwig_version = entity[6]
            old_backend = entity[7]

            logger.warning(f'There was a previous test for {test_name} which yielded better results than the current one. The results were:  \n * Accuracy: {old_accuracy} \n * Batch id: {old_batch_id} \n * Batch started: {old_batch_started} \n * Runtime: {old_runtime} \n * Backend: {old_backend} \n * System info when ran: (mindsdb_version: {old_mindsdb_version}, lightwood_version: {old_lightwood_version}, ludwig_version: {old_ludwig_version})')

    con.commit()
    con.close()


if sys.argv[1] == 'bench':
    run_benchmarks()

if sys.argv[1] == 'compare':
    compare()
