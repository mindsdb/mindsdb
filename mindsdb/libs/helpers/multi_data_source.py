from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_sources.csv_file_ds import CSVFileDS
from pathlib import Path

# import logging
from mindsdb.libs.helpers.logging import logging


def getDS(from_data):
    '''
    Get a datasource give the input
    :param input: a string or an object
    :return: a datasource

    '''

    if isinstance(from_data, DataSource):
        from_ds = from_data
    elif Path(from_data).is_file():
        from_ds = CSVFileDS(from_data)
    else:  # assume is a query
        logging.error('No data matched the input data')

    return from_ds

