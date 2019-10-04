import boto3
from botocore import UNSIGNED
from botocore.client import Config


from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_types.mindsdb_logger import log


class S3DS(DataSource):

    def _setup(self, bucket_name, file_path, access_key=None, secret_key=None):
        if access_key is not None and secret_key is not None:
            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        else:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        tmp_file_name = '.tmp_mindsdb_data_file'
        
        with open(tmp_file_name, 'wb') as fw:
            s3.download_fileobj(bucket_name, file_path, fw)

if __name__ == "__main__":
    ds = S3DS(bucket_name='mindsdb-example-data',file_path='home_rentals.csv', access_key=None, secret_key=None)
