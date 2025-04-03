import os
from typing import Iterable, Callable
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.utilities.context_executor import execute_in_threads


def get_max_thread_count() -> int:
    """
        Calculate the maximum number of threads allowed for the system.
    """
    # workers count
    is_cloud = Config().is_cloud
    if is_cloud:
        max_threads = int(os.getenv('MINDSDB_MAX_PARTITIONING_THREADS', 10))
    else:
        max_threads = os.cpu_count() - 3

    if max_threads < 1:
        max_threads = 1

    return max_threads


def split_data_frame(df: pd.DataFrame, partition_size: int) -> Iterable[pd.DataFrame]:
    """
    Split data frame into chunks with partition_size and yield them out
    """
    chunk = 0
    while chunk * partition_size < len(df):
        # create results with partition
        df1 = df.iloc[chunk * partition_size: (chunk + 1) * partition_size]
        chunk += 1
        yield df1


def process_dataframe_in_partitions(df: pd.DataFrame, callback: Callable, partition_size: int) -> Iterable:
    """
    Splits dataframe into partitions and apply callback on each partition

    :param df: input dataframe
    :param callback: function to apply on each partition
    :param partition_size: size of each partition
    :return: yield results
    """

    # tasks

    tasks = split_data_frame(df, partition_size)

    max_threads = get_max_thread_count()

    chunk_count = int(len(df) / partition_size)
    # don't exceed chunk_count
    if chunk_count > 0:
        max_threads = min(max_threads, chunk_count)

    if max_threads == 1:
        # don't spawn threads

        for task in tasks:
            yield callback(task)

    else:
        for result in execute_in_threads(callback, tasks, thread_count=max_threads):
            yield result
