import pandas as pd
from mindsdb.integrations.handlers.lightfm_handler.helpers import RecommenderPreprocessor
from mindsdb.integrations.handlers.lightfm_handler.lightfm_handler import LightFMHandler
import pytest
from pathlib import Path

TEST_DATA_PATH = Path(__file__).parent.resolve() / 'data'


def get_df(file_name: str) -> pd.DataFrame:
	return pd.read_csv(TEST_DATA_PATH / file_name)


@pytest.fixture
def interaction_data() -> pd.DataFrame:
	return get_df('ratings.csv')


@pytest.fixture
def item_data() -> pd.DataFrame:
	return get_df('movies.csv')


def test_preprocessing_cf(interaction_data, item_data):
	"""Tests helper function for preprocessing"""

	rec_preprocessor = RecommenderPreprocessor(
		interaction_data=interaction_data,
		item_data=item_data,
		user_id_column_name="userId",
		item_id_column_name="movieId",
		item_description_column_name="title"
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


def test_create_light_fm_handler(interaction_data, item_data):
	lfm_handler = LightFMHandler(model_storage='dummy', engine_storage='dummy')

	lfm_handler.create(
		interaction_data,
		item_data,
		dict(
			user_id_column_name="userId",
			item_id_column_name="movieId",
			item_description_column_name="title"
		)
	)


def test_predict_light_fm_handler():
	...
