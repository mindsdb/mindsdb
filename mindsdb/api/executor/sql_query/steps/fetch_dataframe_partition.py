import pandas as pd
import threading
import queue


from mindsdb.api.executor.planner.steps import FetchDataframeStepPartition
from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.partitioning import get_max_thread_count, split_data_frame
from mindsdb.api.executor.sql_query.steps.fetch_dataframe import get_table_alias, get_fill_param_fnc

from .base import BaseStepCall


class FetchDataframePartitionCall(BaseStepCall):

    bind = FetchDataframeStepPartition

    def close_workers(self, workers):
        self.stop_event.set()
        for worker in workers:
            if worker.is_alive():
                worker.join()

    def call(self, step):
        self.dn = self.session.datahub.get(step.integration)
        query = step.query

        # fill params
        fill_params = get_fill_param_fnc(self.steps_data)
        query_traversal(query, fill_params)

        # get query record
        run_query = self.sql_query.run_query
        if run_query is None:
            raise RuntimeError('Error with partitioning of the query')
        run_query.set_params(step.params)

        self.table_alias = get_table_alias(step.query.from_table, self.context.get('database'))
        self.current_step_num = step.step_num
        self.substeps = step.steps

        config = Config()

        # ml task queue enabled?
        use_threads, thread_count = False, None
        if config['ml_task_queue']['type'] == 'redis':
            use_threads = True

        # use threads?
        if 'threads' in step.params:
            threads = step.params['threads']
            if isinstance(threads, int):
                thread_count = threads
                use_threads = True
            if threads is True:
                use_threads = True
            if threads is False:
                # disable even with ml task queue
                use_threads = False

        if use_threads:
            return self.fetch_threads(run_query, query, thread_count=thread_count)
        else:
            return self.fetch_iterate(run_query, query)

    def fetch_iterate(self, run_query, query):
        # process batches in circle

        results = []
        while True:

            # fetch batch
            query2 = run_query.get_partition_query(self.current_step_num, query)
            df, columns_info = self.dn.query(
                query=query2,
                session=self.session
            )

            if df is None or len(df) == 0:
                break

            # executing of sub steps can modify dataframe columns, lets memorise max tracking value
            max_track_value = run_query.get_max_track_value(df)
            sub_data = self.exec_sub_steps(df)

            results.append(sub_data)

            run_query.set_progress(df, max_track_value)

        return self.concat_results(results)

    def concat_results(self, results):
        df_list = []
        for res in results:
            df, col_names = res.to_df_cols()
            if len(df) > 0:
                df_list.append(df)

        data = ResultSet()
        if len(df_list) > 0:
            data.from_df_cols(pd.concat(df_list), col_names)

        return data

    def exec_sub_steps(self, df):
        input_data = ResultSet()

        input_data.from_df(
            df,
            table_name=self.table_alias[1],
            table_alias=self.table_alias[2],
            database=self.table_alias[0]
        )

        # execute with modified previous results
        steps_data2 = self.steps_data.copy()
        steps_data2[self.current_step_num] = input_data

        sub_data = None
        for substep in self.substeps:
            sub_data = self.sql_query.execute_step(substep, steps_data=steps_data2)
            steps_data2[substep.step_num] = sub_data
        return sub_data

    def fetch_threads(self, run_query, query, thread_count=None):
        # process batches in threads

        # create communication queues
        queue_in = queue.Queue()
        queue_out = queue.Queue()
        self.stop_event = threading.Event()

        if thread_count is None:
            thread_count = get_max_thread_count()

        # 3 tasks per worker during 1 batch
        partition_size = int(run_query.batch_size / thread_count / 3)
        # min partition size
        if partition_size < 10:
            partition_size = 10

        # create N workers pool
        workers = []
        results = []

        try:
            for i in range(thread_count):
                worker = threading.Thread(target=self._worker, daemon=True, args=(ctx.dump(), queue_in,
                                                                                  queue_out, self.stop_event))
                worker.start()
                workers.append(worker)

            while True:
                # fetch batch
                query2 = run_query.get_partition_query(self.current_step_num, query)
                df, columns_info = self.dn.query(
                    query=query2,
                    session=self.session
                )

                if df is None or len(df) == 0:
                    # TODO detect circles: data handler ignores condition and output is repeated

                    # exit & stop workers
                    break

                max_track_value = run_query.get_max_track_value(df)

                # split into chunks and send to workers
                sent_chunks = 0
                for df2 in split_data_frame(df, partition_size):
                    queue_in.put(df2)
                    sent_chunks += 1

                for i in range(sent_chunks):
                    res = queue_out.get()
                    if 'error' in res:
                        raise RuntimeError(res['error'])

                    if res['data']:
                        results.append(res['data'])

                # TODO
                #  1. get next batch without updating track_value:
                #    it allows to keep queue_in filled with data between fetching batches
                run_query.set_progress(df, max_track_value)
        finally:
            self.close_workers(workers)

        return self.concat_results(results)

    def _worker(self, context, queue_in, queue_out, stop_event):
        ctx.load(context)
        while True:
            if stop_event.is_set():
                break

            try:
                df = queue_in.get(timeout=1)
                if df is None:
                    continue

                sub_data = self.exec_sub_steps(df)

                queue_out.put({'data': sub_data})
            except queue.Empty:
                continue

            except Exception as e:
                queue_out.put({'error': str(e)})
                stop_event.set()
