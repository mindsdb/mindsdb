import pytest
from pathlib import Path
import pandas as pd

TEST_DATA_PATH = Path(__file__).parent.resolve() / 'data'

def get_df(file_name: str) -> pd.DataFrame:
	return pd.read_csv(TEST_DATA_PATH / file_name)


@pytest.fixture
def interaction_data() -> pd.DataFrame:
	return get_df('ratings.csv')


@pytest.fixture
def item_data() -> pd.DataFrame:
	return get_df('movies.csv')
