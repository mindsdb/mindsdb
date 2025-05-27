from pathlib import Path

import pandas as pd
import pytest

TEST_DATA_PATH = Path(__file__).parent.resolve() / "data"


def get_df(file_name: str, dtype: dict = None) -> pd.DataFrame:
    return pd.read_csv(TEST_DATA_PATH / file_name, dtype=dtype)


@pytest.fixture
def lightfm_interaction_data() -> pd.DataFrame:
    return get_df(
        "ratings.csv", dtype={"userId": "str", "movieId": "str", "rating": "float64"}
    )


@pytest.fixture
def lightfm_item_data() -> pd.DataFrame:
    return get_df(
        "movies.csv", dtypes={"movieId": "str", "title": "str", "genres": "str"}
    )
