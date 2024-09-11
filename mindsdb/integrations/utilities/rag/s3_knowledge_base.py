import re
from typing import List, Dict,Optional

import tempfile
import boto3
from botocore.exceptions import NoCredentialsError
from mindsdb.interfaces.knowledge_base.data_source_config import S3Config


def connect_s3(s3_config: S3Config):
    """
    Establishes a connection to AWS S3 using the provided configuration.

    :param s3_config: S3Config object containing AWS credentials and configuration.
    :return: boto3 S3 client.
    """
    try:
        client_params = {
            'service_name': 's3',
            'aws_access_key_id': s3_config.aws_access_key_id,
            'aws_secret_access_key': s3_config.aws_secret_access_key,
            'region_name': s3_config.region_name,
        }

        # Add session token if provided
        if s3_config.aws_session_token:
            client_params['aws_session_token'] = s3_config.aws_session_token

        return boto3.client(**client_params)
    except NoCredentialsError:
        raise Exception("Credentials not available")

def get_filtered_files(s3_config: S3Config) -> List[Dict]:
    """
    Retrieves and filters files from S3 buckets based on the provided configuration.

    :param s3_config: S3Config object containing bucket and file regex patterns.
    :return: List of dictionaries with filtered file details.
    """
    s3 = connect_s3(s3_config)
    results = []

    # List and filter buckets based on regex patterns
    all_buckets = [bucket['Name'] for bucket in s3.list_buckets().get('Buckets', [])]
    filtered_buckets = [bucket for bucket in all_buckets if any(re.search(pattern, bucket) for pattern in s3_config.buckets)]

    for bucket in filtered_buckets:
        continuation_token = None

        while True:
            # Fetch objects, handle pagination with ContinuationToken
            list_kwargs = {'Bucket': bucket}
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token

            response = s3.list_objects_v2(**list_kwargs)

            if 'Contents' not in response:
                break  # No more contents to process

            # Iterate through objects and filter by file patterns and last modified date
            for obj in response['Contents']:
                key = obj['Key']
                last_modified = obj['LastModified']

                # Check if the file matches any pattern and, if provided, the modification date
                if any(re.search(file_pattern, key) for file_pattern in s3_config.files):
                    if s3_config.update_from_last is None or last_modified <= s3_config.update_from_last:
                        results.append({
                            'bucket': bucket,
                            'key': key,
                            'last_modified': last_modified,
                            'size': obj['Size']
                        })

            # Check if more data needs to be fetched
            continuation_token = response.get('NextContinuationToken')
            if not response.get('IsTruncated'):  # No more data to fetch
                break

    return results





def save_s3_file_to_tempfile(s3_config: S3Config, bucket_name: str, file_key: str) -> Optional[tempfile.NamedTemporaryFile]:
    """
    Downloads a file from S3 and saves it into a temporary file with the correct suffix.

    :param s3_config: S3Config object containing AWS credentials and configuration.
    :param bucket_name: The name of the S3 bucket containing the file.
    :param file_key: The key (path) of the file in the S3 bucket.
    :return: A NamedTemporaryFile object containing the downloaded file content, or None if the file does not exist.
    """
    s3 = connect_s3(s3_config)

    try:
        # Fetch the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)

        # Extract the file extension from the file key
        file_extension = '.' + file_key.split('.')[-1] if '.' in file_key else ''

        # Create a temporary file with the correct suffix
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file_extension)

        # Write the file content to the temporary file
        with temp_file as f:
            f.write(response['Body'].read())

        # Return the temporary file object
        return temp_file

    except s3.exceptions.NoSuchKey:
        raise Exception(f"The file {file_key} does not exist in the bucket {bucket_name}.")
    except s3.exceptions.NoSuchBucket:
        raise Exception(f"The bucket {bucket_name} does not exist.")
    except NoCredentialsError:
        raise Exception("Credentials not available")
    except Exception as e:
        raise Exception(f"An error occurred while retrieving the file: {str(e)}")

