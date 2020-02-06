import json
import sys
import uuid
import os
import shutil
import importlib
import datetime

import requests
import mindsdb
import lightwood
import ludwig

from helpers import *


def run_benchmarks():
    logger = setup_logger()
    con, cur, cfg = get_mysql(sys.argv[1])

    cur.execute("""CREATE DATABASE IF NOT EXISTS mindsdb_accuracy""")
    cur.execute("""CREATE TABLE IF NOT EXISTS mindsdb_accuracy.tests (
        batch_id                Text
        ,batch_started          Datetime
        ,test_name              Text
        ,dataset_name           Text
        ,accuracy               Float
        ,accuracy_function      Text
        ,accuracy_description   Text
        ,runtime                BIGINT
        ,started                Datetime
        ,ended                  Datetime
        ,mindsdb_version        Text
        ,lightwood_version      Text
        ,ludwig_version         Text
        ,backend                Text
        ,label                  Text
    ) ENGINE=InnoDB""")

    batch_id = uuid.uuid4().hex
    batch_started = datetime.datetime.now()

    try:
        os.mkdir('tmp_downloads')
    except:
        pass

    TESTS = ['default_of_credit', 'cancer50', 'pulsar_stars', 'cifar_100', 'imdb_movie_review', 'german_credit_data', 'wine_quality']
    #TESTS = ['default_of_credit', 'cancer50', 'pulsar_stars']

    os.system('git clone https://github.com/mindsdb/mindsdb-examples tmp_downloads')

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
        logger.debug(f'\n\n=================================\nRunning test: {test_name}\n=================================\n\n')

        os.chdir(f'tmp_downloads/benchmarks/{test_name}')

        run_test = importlib.import_module(f'tmp_downloads.benchmarks.{test_name}.mindsdb_acc').run

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
        cur.execute("""INSERT INTO mindsdb_accuracy.tests VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(
            batch_id, batch_started, test_data['test_name'], test_data['dataset_name'],test_data['accuracy'],
            test_data['accuracy_function'], test_data['accuracy_description'],test_data['runtime'], test_data['started'],
            test_data['ended'], mindsdb.__version__, lightwood.__version__, ludwig.__version__, test_data['backend_used'],
            sys.argv[2]
        ))
        con.commit()

    shutil.rmtree('tmp_downloads')

    con.commit()
    con.close()

if __name__ == "__main__":
    run_benchmarks()
