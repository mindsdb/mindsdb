import time
from unittest.mock import patch

import pandas as pd
from mindsdb_sql import parse_sql
from unit.executor_test_base import BaseExecutorTest


# wip
class TestPopularityRecommender(BaseExecutorTest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                if ret["STATUS"][0] == "complete":
                    done = True
                    break
                elif ret["STATUS"][0] == "error":
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_popularity_handler(self, mock_handler, lightfm_interaction_data):

        # create project
        self.run_sql("create database proj")
        self.set_handler(
            mock_handler, name="pg", tables={"df": lightfm_interaction_data}
        )

        # create predictor
        self.run_sql(
            """
            create model proj.modelx
            from pg (select * from df)
            predict movieId
            using
                engine='popularity_recommender',
                item_id='movieId',
                user_id='userId',
                n_recommendations=10
                """
        )
        self.wait_predictor("proj", "modelx")

        result_df = self.run_sql(
            """
            SELECT p.*
            FROM pg.df as t
            JOIN proj.modelx as p
            """
        )

        assert not result_df.empty

        # ensure that we have the right number of recommendations per user id
        assert result_df.userId.value_counts().isin([10]).all()

        # check we have predictions for all user_ids
        assert set(lightfm_interaction_data.userId.unique()) == set(
            result_df.userId.unique()
        )
