import pandas as pd
import time
import pytest
from pathlib import Path
import scipy as sp
from unittest.mock import patch

from mindsdb_sql import parse_sql
from mindsdb.integrations.handlers.lightfm_handler.helpers import RecommenderPreprocessor, item_mapping
from mindsdb.integrations.handlers.lightfm_handler.lightfm_handler import LightFMHandler
from unit.executor_test_base import BaseExecutorTest


def test_preprocessing_cf(interaction_data):
	"""Tests helper function for preprocessing"""

	rec_preprocessor = RecommenderPreprocessor(
		interaction_data=interaction_data,
		user_id_column_name="userId",
		item_id_column_name="movieId"
	)

	rec_preprocessor.encode_interactions()

	interaction_matrix = rec_preprocessor.construct_interaction_matrix()

	# check ids are int64
	assert rec_preprocessor.interaction_data[[rec_preprocessor.user_id_column_name, rec_preprocessor.item_id_column_name]] \
		.dtypes[rec_preprocessor.interaction_data.dtypes == 'int64'].all()

	# check interaction are equal to 1 or -1 e.g. positive or negative
	assert rec_preprocessor.interaction_data['interaction'].apply(lambda x: x == -1 or x == 1).all()

	# check interaction matrix is the expected shape
	assert interaction_matrix.shape == (503, 89)
	assert isinstance(interaction_matrix, sp.sparse.coo_matrix)


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
	def test_create_light_fm_handler(self, mock_handler, interaction_data):
		lfm_handler = LightFMHandler(model_storage='dummy', engine_storage='dummy')

		self.set_handler(mock_handler, name="pg", tables={"df": interaction_data})

		# create project
		self.run_sql("create database proj")


		# todo define model syntax

		# create predictor
		self.run_sql(
			"""
		   
		"""
		)
		self.wait_predictor("proj", "modelx")


	def test_predict_light_fm_handler(self):
		...
