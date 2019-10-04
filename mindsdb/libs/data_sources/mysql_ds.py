import os

import MySQLdb

from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.data_sources.file_ds import FileDS


class MySqlDS(DataSource):

    def _setup(self, bucket_name, file_path, access_key=None, secret_key=None, use_default_credentails=False):
        if access_key is not None and secret_key is not None:
            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        elif use_default_credentails:
            s3 = boto3.client('s3')
        else:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        self.tmp_file_name = '.tmp_mindsdb_data_file'

        with open(self.tmp_file_name, 'wb') as fw:
            s3.download_fileobj(bucket_name, file_path, fw)

        file_ds = FileDS(self.tmp_file_name)
        return file_ds._df, file_ds._col_map

if __name__ == "__main__":
    con = MySQLdb.connect("localhost", "root", "", "")
    cur = con.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS test_mindsdb(col_1 Text, col_2 BIGINT, col_3 BOOL)')
    con.close()

    exit()
    ds = S3DS(bucket_name='mindsdb-example-data',file_path='home_rentals.csv', access_key=None, secret_key=None)
    os.remove(ds.tmp_file_name)
