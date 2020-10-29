import os
import boto3
from pathlib import Path
from bisect import bisect_left

required_env_variables = [
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_REGION',
    'SOURCE_DIR',
    'DEST_DIR',
    'AWS_S3_BUCKET'
]

for var in required_env_variables:
    if os.getenv(var) is None:
        exit('environment variable {} is not set'.format(var))

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)


def list_source_objects(source_folder):
    path = Path(source_folder)
    paths = []
    for file_path in path.rglob('*'):
        if file_path.is_dir():
            continue
        str_file_path = str(file_path)
        str_file_path = str_file_path.replace(f'{str(path)}/', '')
        paths.append(str_file_path)
    return paths


def sync(source, dest, bucket):
    for path in list_source_objects(source_folder=source):
        src_filename = Path(path).name
        print('Uploading {} ({})'.format(path, src_filename))
        s3.upload_file(
            Filename=path,
            Bucket=bucket,
            Key=(Path(dest).joinpath(src_filename).as_posix
        )


if __name__ == '__main__':
    sync(
        source=os.getenv('SOURCE_DIR'),
        dest=os.getenv('DEST_DIR'),
        bucket=os.getenv('AWS_S3_BUCKET')
    )
