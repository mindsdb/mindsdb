import pandas as pd
from mindsdb.integrations.handlers.lightfm_handler.helpers import RecommenderPreprocessor
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
		user_id_column_name="userId",
		item_id_column_name="movieId",
		item_description_column_name="title"
	)

	interactions_encoded_df = rec_preprocessor.encode_interactions()

	interaction_matrix = rec_preprocessor.construct_interaction_matrix()

	# check ids are int64
	assert interactions_encoded_df[[rec_preprocessor.user_id_column_name, rec_preprocessor.item_id_column_name]] \
		.dtypes[interactions_encoded_df.dtypes == 'int64'].all()

	# check interaction are equal to 1 or -1 e.g. positive or negative
	assert interactions_encoded_df['interaction'].apply(lambda x: x == -1 or x == 1).all()

	# check interaction matrix is the expected shape

	#interaction_matrix.shape =


def test_create_light_fm_handler():
		...
