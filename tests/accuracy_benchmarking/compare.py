import sys

from helpers import *


def compare():
    logger = setup_logger()
    con, cur, cfg = get_mysql(sys.argv[1])

    con = MySQLdb.connect(cfg['mysql']['host'], cfg['mysql']['user'], cfg['mysql']['password'], cfg['mysql']['database'])
    cur = con.cursor()

    if len(sys.argv) > 2:
        batch_id = sys.argv[2]
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

        cur.execute('SELECT accuracy, batch_id, batch_started, runtime, mindsdb_version, lightwood_version, ludwig_version, backend FROM mindsdb_accuracy.tests WHERE test_name=%s AND accuracy_function=%s AND (accuracy - 0.0000001) > %s', [test_name,accuracy_function,accuracy])
        better_tests = cur.fetchall()

        for row in better_tests:
            old_accuracy = row[0]
            old_batch_id = row[1]
            old_batch_started = row[2]
            old_runtime = row[3]

            old_mindsdb_version = row[4]
            old_lightwood_version = row[5]
            old_ludwig_version = row[6]
            old_backend = row[7]


            # There's going to be some variation within accuracy, so we only really need to worry if the grap is bigger than 1%
            if old_accuracy > accuracy*0.05:
                worseness_prefix = 'significantly'
                func = logger.error
            elif old_accuracy > accuracy*0.01:
                worseness_prefix = ''
                func = logger.warning
            else:
                worseness_prefix = 'possibly'
                func = logger.debug

            func(f'There was a previous test for {test_name} which yielded {worseness_prefix} better results than the current one. The results were:  \n * Accuracy: {old_accuracy} \n * Batch id: {old_batch_id} \n * Batch started: {old_batch_started} \n * Runtime: {old_runtime} \n * Backend: {old_backend} \n * System info when ran: (mindsdb_version: {old_mindsdb_version}, lightwood_version: {old_lightwood_version}, ludwig_version: {old_ludwig_version})\n')


    con.commit()
    con.close()

if __name__ == "__main__":
    compare()
