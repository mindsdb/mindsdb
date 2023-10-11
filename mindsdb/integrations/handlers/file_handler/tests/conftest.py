import os
import pandas as pd
import pytest
import pyarrow.parquet as pq
import openpyxl
from io import StringIO

@pytest.fixture
def csv_file_path() -> str:
    # Get the current directory of the test file and construct the CSV file path
    test_dir = os.path.dirname(os.path.abspath(__file__))
    csv_filename = "test_data.csv"
    return os.path.join(test_dir, csv_filename)

def check_file_format(file_path, expected_format, checker_function):
    with open(file_path, "rb") as file:
        file_data = file.read()
    
    data_io = StringIO(file_data.decode("utf-8"))
    is_expected_format = checker_function(data_io)
    
    return is_expected_format

def read_csv_file(csv_file_path):
    with open(csv_file_path, "rb") as csv_file:
        csv_data = csv_file.read()
    return csv_data

def assert_not_identified_as(data_format, file_path, is_identified):
    assert not is_identified, f"{file_path} should not be identified as {data_format}"

def assert_identified_as(data_format, file_path, is_identified):
    assert is_identified, f"{file_path} should be identified as {data_format}"