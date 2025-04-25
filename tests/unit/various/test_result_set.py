import pytest
from pandas import DataFrame, NA

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES, MYSQL_DATA_TYPE, DATA_C_TYPE_MAP, CTypeProperties

int_tests = [{
    'input': [1, 2, 3],
    'dtype': 'int64',
    'output': ['1', '2', '3'],
    'mysql_type': MYSQL_DATA_TYPE.INT  #,TYPES.MYSQL_TYPE_LONG
}, {
    'input': [1, NA, None],
    'dtype': 'Int64',
    'output': ['1', None, None],
    'mysql_type': MYSQL_DATA_TYPE.INT  #TYPES.MYSQL_TYPE_LONG
}, {
    'input': [1, NA, None],
    'dtype': 'object',
    'output': ['1', None, None],
    'mysql_type': MYSQL_DATA_TYPE.TEXT # TYPES.MYSQL_TYPE_LONG
}, {
    'input': ['1', NA, None],
    'dtype': 'object',
    'output': ['1', None, None],
    'mysql_type': MYSQL_DATA_TYPE.TEXT
}]


@pytest.mark.parametrize('test_index, test_case', enumerate(int_tests))
def test_mysql_dump_int(test_index: int, test_case: dict):
    df = DataFrame(test_case['input'], columns=['a'], dtype=test_case['dtype'])
    rs = ResultSet.from_df(df, mysql_types=None)
    df, columns = rs.dump_to_mysql()
    type_attrs = DATA_C_TYPE_MAP[test_case['mysql_type']]
    for result_attr, expected_attr in [('type', 'code'), ('size', 'size'), ('flags', 'flags')]:
        assert columns[0][result_attr] == getattr(type_attrs, expected_attr), (
            f'Test case {test_index}: '
            f'Wrong mysql type attribute "{result_attr}" for {test_case["input"]}:{test_case["dtype"]}: '
            f'{columns[0][result_attr]} (result) != {getattr(type_attrs, expected_attr)} (expected)'
        )
    for i in range(len(test_case['input'])):
        assert df[0][i] == test_case['output'][i], (
            f'Test case {test_index}: Wrong cast for {test_case["input"][i]} -> {test_case["output"][i]}'
        )
