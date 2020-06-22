import mysql.connector
import datetime
import logging
from mindsdb import Predictor
from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES
from mindsdb.libs.data_sources.maria_ds import MariaDS


def test_maria_ds():
    HOST = 'localhost'
    USER = 'root'
    PASSWORD = ''
    DATABASE = 'mysql'
    PORT = 4306

    con = mysql.connector.connect(host=HOST,
                                  port=PORT,
                                  user=USER,
                                  password=PASSWORD,
                                  database=DATABASE)
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS test_mindsdb')
    cur.execute('CREATE TABLE test_mindsdb(col_categorical Text, col_int BIGINT, col_bool BOOL, col_float FLOAT, col_date Text, col_ts Text, col_text Text)')
    for i in range(0, 200):
        dt = datetime.datetime.now() - datetime.timedelta(days=i)
        dt_str = dt.strftime('%Y-%m-%d')
        ts = (datetime.datetime.now() - datetime.timedelta(minutes=i)).isoformat()
        cur.execute(f'INSERT INTO test_mindsdb VALUES ("String {i}", {i}, {i % 2 == 0}, {i+0.01}, "{dt_str}", "{ts}", "long long long text {i}")')
    con.commit()
    con.close()

    maria_ds = MariaDS(table='test_mindsdb', host=HOST, user=USER,
                       password=PASSWORD, database=DATABASE, port=PORT)
    assert (len(maria_ds._df) == 200)

    mdb = Predictor(name='analyse_dataset_test_predictor', log_level=logging.ERROR)
    model_data = mdb.analyse_dataset(from_data=maria_ds)
    analysis = model_data['data_analysis_v2']
    assert model_data
    assert analysis

    assert analysis['col_categorical']['typing']['data_type'] == DATA_TYPES.CATEGORICAL
    assert analysis['col_categorical']['typing']['data_subtype'] == DATA_SUBTYPES.MULTIPLE

    assert analysis['col_int']['typing']['data_type'] == DATA_TYPES.NUMERIC
    assert analysis['col_int']['typing']['data_subtype'] == DATA_SUBTYPES.INT

    assert analysis['col_float']['typing']['data_type'] == DATA_TYPES.NUMERIC
    assert analysis['col_float']['typing']['data_subtype'] == DATA_SUBTYPES.FLOAT

    assert analysis['col_bool']['typing']['data_type'] == DATA_TYPES.CATEGORICAL
    assert analysis['col_bool']['typing']['data_subtype'] == DATA_SUBTYPES.SINGLE

    assert analysis['col_date']['typing']['data_type'] == DATA_TYPES.DATE
    assert analysis['col_date']['typing']['data_subtype'] == DATA_SUBTYPES.DATE

    assert analysis['col_ts']['typing']['data_type'] == DATA_TYPES.DATE
    assert analysis['col_ts']['typing']['data_subtype'] == DATA_SUBTYPES.TIMESTAMP

    assert analysis['col_text']['typing']['data_type'] == DATA_TYPES.SEQUENTIAL
    assert analysis['col_text']['typing']['data_subtype'] == DATA_SUBTYPES.TEXT
