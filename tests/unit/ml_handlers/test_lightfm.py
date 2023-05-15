import pandas as pd
import time
import pytest
from pathlib import Path
import scipy as sp
from unittest.mock import patch

from mindsdb_sql import parse_sql
from mindsdb.integrations.handlers.lightfm_handler.helpers import RecommenderPreprocessor
from mindsdb.integrations.handlers.lightfm_handler.lightfm_handler import LightFMHandler
from unit.executor_test_base import BaseExecutorTest


def test_preprocessing_cf(interaction_data):
	"""Tests helper function for preprocessing"""

	rec_preprocessor = RecommenderPreprocessor(
		interaction_data=interaction_data,
		user_id_column_name="userId",
		item_id_column_name="movieId"
	)

	preprocessed_data = rec_preprocessor.preprocess()

	# check ids are int64
	assert preprocessed_data.interaction_df[[rec_preprocessor.user_id_column_name, rec_preprocessor.item_id_column_name]] \
		.dtypes[preprocessed_data.interaction_df.dtypes == 'int64'].all()

	# check interaction are equal to 1 or -1 e.g. positive or negative
	assert preprocessed_data.interaction_df['interaction'].apply(lambda x: x == -1 or x == 1).all()

	print("d")
	# check interaction matrix is the expected shape
	assert preprocessed_data.interaction_matrix.shape == (503, 89)
	assert isinstance(preprocessed_data.interaction_matrix, sp.sparse.coo_matrix)


class TestLightFM(BaseExecutorTest):

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
			columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
			return pd.DataFrame(ret.data, columns=columns)

	@patch("mindsdb.integrations.handlers.postgres_handler.Handler")
	def test_collaborative_filter_user_item_recommendation_light_fm_handler(self, mock_handler, interaction_df):

		self.set_handler(mock_handler, name="pg", tables={"df": interaction_df})

		# create project
		self.run_sql("create database proj")

		# create predictor
		self.run_sql(
			"""
			create model proj.useritemtest
			from pg (select * from df)
			predict movieId
			using
				engine='lightfm',
				item_id='movieId',
				user_id='userId',
				recommendation_type='user_item',
				threshold=4,
				n_recommendations=10
				"""
		)
		self.wait_predictor("proj", "useritemtest")

		result_df = self.run_sql(
			"""
		   SELECT p.*
		   FROM pg.df as t
		   JOIN proj.useritemtest as p
		"""
		)

		# check that the result is the expected shape e.g. 10 recommendations per user  * 503 users
		assert result_df.shape == (5030, 3)


	@patch("mindsdb.integrations.handlers.postgres_handler.Handler")
	def test_collaborative_filter_item_item_recommendation_light_fm_handler(self, mock_handler, interaction_df):

		self.set_handler(mock_handler, name="pg", tables={"df": interaction_df})

		# create project
		self.run_sql("create database proj")

		# create predictor
		self.run_sql(
			"""
			create model proj.itemitemtest
			from pg (select * from df)
			predict movieId
			using
				engine='lightfm',
				item_id='movieId',
				user_id='userId',
				threshold=4,
				recommendation_type='item_item',
				similar_to_item='1',
				n_recommendations=10
				
				"""
		)
		self.wait_predictor("proj", "itemitemtest")

		result_df = self.run_sql(
			"""
		   SELECT p.*
		   FROM pg.df as t
		   JOIN proj.itemitemtest as p
		"""
		)

		# check that the result is the expected shape e.g. 10 recommendations per user  * 503 users
		assert result_df.shape == (10, 2)



