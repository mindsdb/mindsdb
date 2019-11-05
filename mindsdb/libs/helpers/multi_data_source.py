from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_sources.file_ds import FileDS
from pandas import DataFrame

from mindsdb.libs.data_types.mindsdb_logger import log


def getDS(from_data):
    '''
    Get a datasource give the input

    :param input: a string or an object
    :return: a datasource
    '''

    if isinstance(from_data, DataSource):
        from_ds = from_data

    elif isinstance(from_data, DataFrame):
        from_ds = DataSource(from_data)


    else:  # assume is a file
        from_ds = FileDS(from_data)
        if from_ds is None:
            log.error('No data matched the input data')

    return from_ds
