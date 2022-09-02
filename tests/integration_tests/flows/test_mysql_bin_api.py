import unittest
import json
import mysql.connector

from pathlib import Path

from common import (
    run_environment,
    EXTERNAL_DB_CREDENTIALS,
    CONFIG_PATH)

from test_mysql_api import TestScenario, Dlist


class MySqlBinApiTest(unittest.TestCase, TestScenario):

    @classmethod
    def setUpClass(cls):

        run_environment(apis=['http', 'mysql'])

        cls.config = json.loads(Path(CONFIG_PATH).read_text())

        with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
            cls.db_creds = json.load(f)

    def query(self, _query, encoding='utf-8'):

        cnx = mysql.connector.connect(
            host=self.config["api"]["mysql"]["host"],
            port=self.config["api"]["mysql"]["port"],
            user=self.config["api"]["mysql"]["user"],
            database='mindsdb',
            password=self.config["api"]["mysql"]["password"]
        )
        cursor = cnx.cursor(prepared=True)

        for subquery in _query.split(';'):
            # multiple queries in one string
            if subquery.strip() == '':
                continue
            cursor.execute(subquery)

        if cursor.description:
            columns = [i[0] for i in cursor.description]
            data = cursor.fetchall()

            res = Dlist()
            for row in data:
                res.append(dict(zip(columns, row)))

        else:
            res = {}

        # print(f'==query==\n {_query}')
        # print(f'==result==\n {res}')
        return res

    def test_8_1_tableau_queries(self):
        test_ds_name = self.file_datasource_name
        predictor_name = "predictor_from_file"
        integration = "files"

        queries = [
            f'''
               SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),
               TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA LIKE '{integration}'
                AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' ) ORDER BY TABLE_SCHEMA, TABLE_NAME
            ''',
            f'''
                SELECT SUM(1) AS `cnt__0B4A4E8BD11C48FFB4730D4D2C32191A_ok`,
                  max(`Custom SQL Query`.`x1`) AS `sum_height_ok`,
                  max(`Custom SQL Query`.`y`) AS `sum_length1_ok`
                FROM (
                  SELECT res.x1, res.y 
                   FROM files.{test_ds_name} as source
                   JOIN mindsdb.{predictor_name} as res
                ) `Custom SQL Query`
                HAVING (COUNT(1) > 0)
            ''',
            f'''
                SHOW FULL TABLES FROM {integration}
            ''',
            '''
                SELECT `table_name`, `column_name`
                FROM `information_schema`.`columns`
                WHERE `data_type`='enum' AND `table_schema`='views';
            ''',
            '''
                SHOW KEYS FROM `mindsdb`.`predictors`
            ''',
            '''
                show full columns from `predictors`
            ''',
            '''
                SELECT `table_name`, `column_name` FROM `information_schema`.`columns`
                 WHERE `data_type`='enum' AND `table_schema`='mindsdb'
            ''',
            f'''
                SELECT `Custom SQL Query`.`x1` AS `height`,
                  `Custom SQL Query`.`y` AS `length1`
                FROM (
                   SELECT res.x1, res.y 
                   FROM files.{test_ds_name} as source
                   JOIN mindsdb.{predictor_name} as res
                ) `Custom SQL Query`
                LIMIT 100
            ''',
            f'''
            SELECT 
              `Custom SQL Query`.`x1` AS `x1`,
              SUM(`Custom SQL Query`.`y2`) AS `sum_y2_ok`
            FROM (
               SELECT res.x1, res.y as y2 
               FROM files.{test_ds_name} as source
               JOIN mindsdb.{predictor_name} as res
            ) `Custom SQL Query`
            GROUP BY 1
            ''',
            f'''
            SELECT 
              `Custom SQL Query`.`x1` AS `x1`,
              COUNT(DISTINCT TRUNCATE(`Custom SQL Query`.`y`,0)) AS `ctd_y_ok`
            FROM (
               SELECT res.x1, res.y
               FROM files.{test_ds_name} as source
               JOIN mindsdb.{predictor_name} as res
            ) `Custom SQL Query`
            GROUP BY 1
            ''',
        ]
        for _query in queries:
            self.query(_query)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True, verbosity=2)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
