import pandas as pd
from mindsdb.integrations.utilities.time_series_utils import (
    transform_to_nixtla_df,
    get_results_from_nixtla_df,
    infer_frequency,
)


def create_mock_df(freq="Q-DEC"):
    df2 = pd.DataFrame(pd.date_range(start="1/1/2010", periods=31, freq=freq), columns=["time_col"])
    df3 = df2.copy()

    df2["target_col"] = range(1, 32)
    df2["group_col"] = "a"
    df2["group_col_2"] = "a2"
    df2["group_col_3"] = "a3"

    df3["target_col"] = range(11, 42)
    df3["group_col"] = "b"
    df3["group_col_2"] = "b2"
    df3["group_col_3"] = "b3"

    return pd.concat([df2, df3]).reset_index(drop=True)


def test_infer_frequency():
    df = create_mock_df()
    assert infer_frequency(df, "time_col") == "Q-DEC"

    df = create_mock_df(freq="M")
    assert infer_frequency(df, "time_col") == "M"

    # Should still work if we pass string dates
    df["time_col"] = df["time_col"].astype(str)
    assert infer_frequency(df, "time_col") == "M"

    # Should still work if we pass unordered dates
    unordered_df = pd.concat([df.iloc[:3, :], df.iloc[3:, :]])
    assert infer_frequency(unordered_df, "time_col") == "M"


def test_statsforecast_df_transformations():
    df = create_mock_df()
    model_name = "ARIMA"
    settings_dict = {
        "order_by": "time_col",
        "group_by": ["group_col"],
        "target": "target_col",
        "model_name": model_name,
    }

    # Test transform for single groupby
    nixtla_df = transform_to_nixtla_df(df, settings_dict)
    assert [nixtla_df["unique_id"].iloc[i] == df["group_col"].iloc[i] for i in range(len(nixtla_df))]
    assert [nixtla_df["y"].iloc[i] == df["target_col"].iloc[i] for i in range(len(nixtla_df))]
    assert [nixtla_df["ds"].iloc[i] == df["time_col"].iloc[i] for i in range(len(nixtla_df))]
    # Test reversing the transformation
    nixtla_results_df = nixtla_df.rename({"y": model_name}, axis=1).set_index("unique_id")
    mindsdb_results_df = get_results_from_nixtla_df(nixtla_results_df, settings_dict)
    pd.testing.assert_frame_equal(mindsdb_results_df, df[["time_col", "target_col", "group_col"]])

    # Test for multiple groups
    settings_dict["group_by"] = ["group_col", "group_col_2", "group_col_3"]
    nixtla_df = transform_to_nixtla_df(df, settings_dict)
    assert nixtla_df["unique_id"][0] == "a|a2|a3"
    # Test reversing the transformation
    nixtla_results_df = nixtla_df.rename({"y": model_name}, axis=1).set_index("unique_id")
    mindsdb_results_df = get_results_from_nixtla_df(nixtla_results_df, settings_dict)
    pd.testing.assert_frame_equal(mindsdb_results_df, df)

    # Test with exogenous vars
    settings_dict["group_by"] = ["group_col"]
    settings_dict["exogenous_vars"] = ["group_col_2", "group_col_3"]
    nixtla_df = transform_to_nixtla_df(df, settings_dict, exog_vars=["group_col_2", "group_col_3"])
    assert nixtla_df.columns.tolist() == ["unique_id", "ds", "y", "group_col_2", "group_col_3"]
