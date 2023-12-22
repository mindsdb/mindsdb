import urllib
import tempfile
from pathlib import Path

import requests


# def get_column_in_case(columns, name):
#     '''
#     '''
#     candidates = []
#     name_lower = name.lower()
#     for column in columns:
#         if column.lower() == name_lower:
#             candidates.append(column)
#     if len(candidates) != 1:
#         return None
#     return candidates[0]


def download_file(url):
    try:
        parse_result = urllib.parse.urlparse(url)
        scheme = parse_result.scheme
    except ValueError:
        raise Exception(f'Invalid url: {url}')
    except Exception as e:
        raise Exception(f'URL parsing error: {e}')
    temp_dir = tempfile.mkdtemp(prefix='mindsdb_file_download_')
    if scheme == '':
        raise Exception(f"Unknown url schema: {url}")

    response = requests.get(url)
    temp_file_path = Path(temp_dir).joinpath('file')
    with open(str(temp_file_path), 'wb')as file:
        file.write(response.content)
    return str(temp_file_path)
