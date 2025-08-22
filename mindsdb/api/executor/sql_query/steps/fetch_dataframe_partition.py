import pandas as pd
from typing import List

from mindsdb_sql_parser import ASTNode
from mindsdb.api.executor.planner.steps import FetchDataframeStepPartition
from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.interfaces.query_context.context_controller import RunningQuery
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.partitioning import get_max_thread_count, split_data_frame
from mindsdb.api.executor.sql_query.steps.fetch_dataframe import get_table_alias, get_fill_param_fnc
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor


from .base import BaseStepCall


logger = log.getLogger(__name__)


class FetchDataframePartitionCall(BaseStepCall):
    """
    Alternative to FetchDataframeCall but fetch data by batches wrapping user's query to:

     select * from ({user query})
      where {track_column} > {previous value}
      order by track_column
      limit size {batch_size} `

    """

    bind = FetchDataframeStepPartition

    def call(self, step: FetchDataframeStepPartition) -> ResultSet:
        """
        Parameters:
        - batch_size - count of rows to fetch from database per iteration, optional default 1000
        - threads - run partitioning in threads, bool or int, optinal, if set:
           - int value: use this as count of threads
           - true: table threads, autodetect count of thread
           - false: disable threads even if ml task queue is enabled
        - track_column - column used for creating partitions
          - query will be sorted by this column and select will be limited by batch_size
        - error (default raise)
          - when `error='skip'`, errors in partition will be skipped and execution will be continued
        """

        self.dn = self.session.datahub.get(step.integration)
        query = step.query

        # fill params
        fill_params = get_fill_param_fnc(self.steps_data)
        query_traversal(query, fill_params)

        # get query record
        run_query = self.sql_query.run_query
        if run_query is None:
            raise RuntimeError("Error with partitioning of the query")
        run_query.set_params(step.params)

        self.table_alias = get_table_alias(step.query.from_table, self.context.get("database"))
        self.current_step_num = step.step_num
        self.substeps = step.steps

        # ml task queue enabled?
        use_threads, thread_count = False, None
        if config["ml_task_queue"]["type"] == "redis":
            use_threads = True

        # use threads?
        if "threads" in step.params:
            threads = step.params["threads"]
            if isinstance(threads, int):
                thread_count = threads
                use_threads = True
            if threads is True:
                use_threads = True
            if threads is False:
                # disable even with ml task queue
                use_threads = False

        on_error = step.params.get("error", "raise")
        if use_threads:
            return self.fetch_threads(run_query, query, thread_count=thread_count, on_error=on_error)
        else:
            return self.fetch_iterate(run_query, query, on_error=on_error)

    def fetch_iterate(self, run_query: RunningQuery, query: ASTNode, on_error: str = None) -> ResultSet:
        """
        Process batches one by one in circle
        """

        results = []

        for df in run_query.get_partitions(self.dn, self, query):
            try:
                sub_data = self.exec_sub_steps(df)
                run_query.set_progress(processed_rows=len(df))
                results.append(sub_data)
            except Exception as e:
                if on_error == "skip":
                    logger.error(e)
                else:
                    raise e

        return self.concat_results(results)

    def concat_results(self, results: List[ResultSet]) -> ResultSet:
        """
        Concatenate list of result sets to single result set
        """
        df_list = []
        for res in results:
            df, col_names = res.to_df_cols()
            if len(df) > 0:
                df_list.append(df)

        data = ResultSet()
        if len(df_list) > 0:
            data = ResultSet.from_df_cols(pd.concat(df_list), col_names)

        return data

    def exec_sub_steps(self, df: pd.DataFrame) -> ResultSet:
        """
        FetchDataframeStepPartition has substeps defined
        Every batch of data have to be used to execute these substeps
        - batch of data is put as result of FetchDataframeStepPartition
        - substep are executed using result of previos step (like it is all fetched data is available)
        - the final result is returned and used outside to concatenate with results of other's batches
        """
        input_data = ResultSet.from_df(
            df, table_name=self.table_alias[1], table_alias=self.table_alias[2], database=self.table_alias[0]
        )

        if len(self.substeps) == 0:
            return input_data

        # execute with modified previous results
        steps_data2 = self.steps_data.copy()
        steps_data2[self.current_step_num] = input_data

        sub_data = None
        for substep in self.substeps:
            sub_data = self.sql_query.execute_step(substep, steps_data=steps_data2)
            steps_data2[substep.step_num] = sub_data
        return sub_data

    def fetch_threads(
        self, run_query: RunningQuery, query: ASTNode, thread_count: int = None, on_error: str = None
    ) -> ResultSet:
        """
        Process batches in threads
        - spawn required count of threads
        - create in/out queue to communicate with threads
        - send task to threads and receive results
        """

        # create communication queues

        if thread_count is None:
            thread_count = get_max_thread_count()

        # 3 tasks per worker during 1 batch
        partition_size = int(run_query.batch_size / thread_count)
        # min partition size
        if partition_size < 10:
            partition_size = 10

        results = []

        with ContextThreadPoolExecutor(max_workers=thread_count) as executor:
            for df in run_query.get_partitions(self.dn, self, query):
                # split into chunks and send to workers
                futures = []
                for df2 in split_data_frame(df, partition_size):
                    futures.append([executor.submit(self.exec_sub_steps, df2), len(df2)])

                error = None
                for future, rows_count in futures:
                    try:
                        results.append(future.result())
                        run_query.set_progress(processed_rows=rows_count)
                    except Exception as e:
                        if on_error == "skip":
                            logger.error(e)
                        else:
                            executor.shutdown()
                            error = e

                if error:
                    raise error
                if self.sql_query.stop_event is not None and self.sql_query.stop_event.is_set():
                    executor.shutdown()
                    raise RuntimeError("Query is interrupted")

        return self.concat_results(results)
