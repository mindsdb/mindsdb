from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_sources.file_ds import FileDS
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

    else:  # assume is a file
        from_ds = FileDS(from_data)
        if from_ds is None:
            logging.error('No data matched the input data')

    return from_ds

