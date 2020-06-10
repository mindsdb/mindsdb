import os

import boto3
from botocore import UNSIGNED
from botocore.client import Config


from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_sources.file_ds import FileDS


class S3DS(DataSource):

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

    def _cleanup(self):
        os.remove(self.tmp_file_name)

if __name__ == "__main__":
    from mindsdb import Predictor
    mdb = Predictor(name='analyse_dataset_test_predictor')
    s3_ds = S3DS(bucket_name='mindsdb-example-data',file_path='home_rentals.csv', access_key=None, secret_key=None)
    mdb.analyse_dataset(from_data=s3_ds)
