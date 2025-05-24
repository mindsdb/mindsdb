from copy import deepcopy
import datetime
from decimal import Decimal

import pytest
from pandas import DataFrame, NA, Timestamp

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE, DATA_C_TYPE_MAP
from mindsdb.api.mysql.mysql_proxy.utilities.dump import dump_result_set_to_mysql


# Test cases without specifying column's mysql_type.
# In this case output mysql_type should be determined by the input dataframe.dtypes.
dtype_tests = [
    # Datetime types
    {
        'input': [
            datetime.date(2023, 10, 15),
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            None
        ],
        'dtype': 'datetime64[ns]',
        'output': [
            datetime.date(2023, 10, 15).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.datetime(2023, 10, 16, 12, 30).strftime("%Y-%m-%d %H:%M:%S"),
            Timestamp('2023-10-15 14:30:45.123456789').strftime("%Y-%m-%d %H:%M:%S"),
            None
        ],
        'mysql_type': MYSQL_DATA_TYPE.DATETIME
    },
    {
        'input': [
            datetime.date(2023, 10, 15),
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            datetime.time(14, 30, 45, 123456),
            None
        ],
        'dtype': 'object',
        'output': [
            str(datetime.date(2023, 10, 15)),
            str(datetime.datetime(2023, 10, 16, 12, 30)),
            str(Timestamp('2023-10-15 14:30:45.123456789')),
            str(datetime.time(14, 30, 45, 123456)),
            None
        ],
        'mysql_type': MYSQL_DATA_TYPE.TEXT
    },

    # BOOL types
    {
        # None is True in dataframe with dtype=bool, we can't change it
        'input': [1, 2, 0, -1, None],
        'dtype': 'bool',
        'output': ['1', '1', '0', '1', '1'],
        'mysql_type': MYSQL_DATA_TYPE.BOOL
    },

    # FLOAT types
    {
        'input': [1, 2, 3],
        'dtype': 'float64',
        'output': ['1.0', '2.0', '3.0'],
        'mysql_type': MYSQL_DATA_TYPE.FLOAT
    }, {
        'input': [1.1, 2.2, 3.3, Decimal('4.4')],
        'dtype': 'float64',
        'output': ['1.1', '2.2', '3.3', '4.4'],
        'mysql_type': MYSQL_DATA_TYPE.FLOAT
    }, {
        'input': [1.1, NA, None, Decimal('4.4')],
        'dtype': 'Float64',
        'output': ['1.1', None, None, '4.4'],
        'mysql_type': MYSQL_DATA_TYPE.FLOAT
    },

    # INT types
    {
        'input': [1, 2, 3],
        'dtype': 'int64',
        'output': ['1', '2', '3'],
        'mysql_type': MYSQL_DATA_TYPE.INT
    }, {
        'input': [1, NA, None],
        'dtype': 'Int64',
        'output': ['1', None, None],
        'mysql_type': MYSQL_DATA_TYPE.INT
    },

    # STR types
    {
        'input': ['a', 1, NA, None, Decimal('4.4')],
        'dtype': 'object',
        'output': ['a', '1', None, None, '4.4'],
        'mysql_type': MYSQL_DATA_TYPE.TEXT
    }, {
        'input': ['a', 1, NA, None, Decimal('4.4')],
        'dtype': 'string',
        'output': ['a', '1', None, None, '4.4'],
        'mysql_type': MYSQL_DATA_TYPE.TEXT
    }, {
        'input': [1, 2, 3],
        'dtype': 'string',
        'output': ['1', '2', '3'],
        'mysql_type': MYSQL_DATA_TYPE.TEXT
    }, {
        'input': [1, 2, 3],
        'dtype': 'object',
        'output': ['1', '2', '3'],
        'mysql_type': MYSQL_DATA_TYPE.TEXT
    }, {
        'input': [1, NA, None],
        'dtype': 'object',
        'output': ['1', None, None],
        'mysql_type': MYSQL_DATA_TYPE.TEXT
    }, {
        'input': ['1', NA, None],
        'dtype': 'object',
        'output': ['1', None, None],
        'mysql_type': MYSQL_DATA_TYPE.TEXT
    },
]

# Test cases with specifying column's mysql_type.
# In this case output mysql_type should be the same as the input mysql_type.
column_types_tests = []


def _map_test_case(test_cases: list[dict], column_types: list[MYSQL_DATA_TYPE]):
    """Make test cases for each column type. Some test cases may be used with group of similar column types
    (like INT, BIGINT, SMALLINT etc.), so we clone test cases for each column type.

    Args:
        test_cases: List of test cases.
        column_types: List of column types.
    """
    for column_type in column_types:
        for test_case in test_cases:
            test_case = deepcopy(test_case)
            test_case['column_type'] = column_type
            test_case['mysql_type'] = column_type
            column_types_tests.append(test_case)


str_test_cases = [
    {
        'input': ['a', 1, NA, None, Decimal('4.4')],
        'dtype': 'object',
        'output': ['a', '1', None, None, '4.4']
    }, {
        'input': ['a', 1, NA, None, Decimal('4.4')],
        'dtype': 'string',
        'output': ['a', '1', None, None, '4.4']
    }, {
        'input': [1, 2, 3],
        'dtype': 'string',
        'output': ['1', '2', '3']
    }, {
        'input': [1, 2, 3],
        'dtype': 'int64',
        'output': ['1', '2', '3']
    }, {
        'input': [1, 2, 3],
        'dtype': 'object',
        'output': ['1', '2', '3']
    }, {
        'input': [1, NA, None],
        'dtype': 'object',
        'output': ['1', None, None]
    }, {
        'input': ['1', NA, None],
        'dtype': 'object',
        'output': ['1', None, None]
    },
]
_map_test_case(str_test_cases, [
    MYSQL_DATA_TYPE.CHAR,
    MYSQL_DATA_TYPE.VARCHAR,
    MYSQL_DATA_TYPE.TINYTEXT,
    MYSQL_DATA_TYPE.TEXT,
    MYSQL_DATA_TYPE.MEDIUMTEXT,
    MYSQL_DATA_TYPE.LONGTEXT
])

bool_test_cases = [
    {
        'input': [1, 2, 0, -1, None],
        'dtype': 'bool',
        'output': ['1', '1', '0', '1', '1'],
    },
    {
        'input': [1, 2, 0, -1, None, 'a', NA],
        'dtype': 'object',
        'output': ['1', '1', '0', '1', None, '1', None],
    }
]
_map_test_case(bool_test_cases, [
    MYSQL_DATA_TYPE.BOOL,
    MYSQL_DATA_TYPE.BOOLEAN
])

float_test_cases = [
    {
        'input': [1, 2, 3],
        'dtype': 'float64',
        'output': ['1.0', '2.0', '3.0']
    }, {
        'input': [1.1, 2.2, 3.3, Decimal('4.4')],
        'dtype': 'float64',
        'output': ['1.1', '2.2', '3.3', '4.4']
    }, {
        'input': [1.1, NA, None, Decimal('4.4')],
        'dtype': 'Float64',
        'output': ['1.1', None, None, '4.4']
    }, {
        'input': [1, 2, 3],
        'dtype': 'object',
        'output': ['1', '2', '3']
    }, {
        'input': [1.1, 2.2, 3.3, Decimal('4.4')],
        'dtype': 'object',
        'output': ['1.1', '2.2', '3.3', '4.4']
    }, {
        'input': [1.1, NA, None, Decimal('4.4')],
        'dtype': 'object',
        'output': ['1.1', None, None, '4.4']
    }
]
_map_test_case(float_test_cases, [
    MYSQL_DATA_TYPE.FLOAT,
    MYSQL_DATA_TYPE.DOUBLE,
    MYSQL_DATA_TYPE.DECIMAL
])

int_test_cases = [
    {
        'input': [1, 2, -3],
        'dtype': 'int64',
        'output': ['1', '2', '-3']
    },
    {
        'input': [1, '2', -3],
        'dtype': 'object',
        'output': ['1', '2', '-3']
    },
    {
        'input': ['1', NA, None],
        'dtype': 'object',
        'output': ['1', None, None]
    },
    {
        'input': ['1', NA, None],
        'dtype': 'Int64',
        'output': ['1', None, None]
    }
]
_map_test_case(int_test_cases, [
    MYSQL_DATA_TYPE.TINYINT,
    MYSQL_DATA_TYPE.SMALLINT,
    MYSQL_DATA_TYPE.MEDIUMINT,
    MYSQL_DATA_TYPE.INT,
    MYSQL_DATA_TYPE.BIGINT
])

dt_test_cases = [
    # with TZ
    {
        'input': [
            datetime.datetime(2023, 1, 1, 15, 30, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=3))),
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 9:30:45.123456789', tz='Atlantic/Stanley'),  # -3
            datetime.time(15, 30, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=3))),
            datetime.time(12, 30, 45),
            None
        ],
        'dtype': 'object',
        'output': [
            '12:30:45',
            '12:30:00',
            '12:30:45',
            '12:30:45',
            '12:30:45',
            None
        ],
        'column_type': MYSQL_DATA_TYPE.TIME,
        'mysql_type': MYSQL_DATA_TYPE.TIME
    },
    {
        'input': [
            datetime.datetime(2023, 1, 1, 15, 30, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=3))),
            datetime.datetime(2023, 10, 16, 12, 30),
            datetime.date(2023, 10, 16),
            Timestamp('2023-10-15 9:30:45.123456789', tz='Atlantic/Stanley'),  # -3
            None
        ],
        'dtype': 'object',
        'output': [
            '2023-01-01 12:30:45',
            '2023-10-16 12:30:00',
            '2023-10-16 00:00:00',
            '2023-10-15 12:30:45',
            None
        ],
        'column_type': MYSQL_DATA_TYPE.DATETIME,
        'mysql_type': MYSQL_DATA_TYPE.DATETIME
    },
    # no TZ
    {
        'input': [
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            None
        ],
        'dtype': 'object',
        'output': [
            datetime.datetime(2023, 10, 16, 12, 30).strftime("%H:%M:%S"),
            Timestamp('2023-10-15 14:30:45.123456789').strftime("%H:%M:%S"),
            None
        ],
        'column_type': MYSQL_DATA_TYPE.TIME,
        'mysql_type': MYSQL_DATA_TYPE.TIME
    },
    {
        'input': [
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            None
        ],
        'dtype': 'datetime64[ns]',
        'output': [
            datetime.datetime(2023, 10, 16, 12, 30).strftime("%H:%M:%S"),
            Timestamp('2023-10-15 14:30:45.123456789').strftime("%H:%M:%S"),
            None
        ],
        'column_type': MYSQL_DATA_TYPE.TIME,
        'mysql_type': MYSQL_DATA_TYPE.TIME
    },
    {
        'input': [
            datetime.date(2023, 10, 15),
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            None
        ],
        'dtype': 'datetime64[ns]',
        'output': [
            datetime.date(2023, 10, 15).strftime("%Y-%m-%d"),
            datetime.datetime(2023, 10, 16, 12, 30).strftime("%Y-%m-%d"),
            Timestamp('2023-10-15 14:30:45.123456789').strftime("%Y-%m-%d"),
            None
        ],
        'column_type': MYSQL_DATA_TYPE.DATE,
        'mysql_type': MYSQL_DATA_TYPE.DATE
    },
    {
        'input': [
            datetime.date(2023, 10, 15),
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            None
        ],
        'dtype': 'object',
        'output': [
            datetime.date(2023, 10, 15).strftime("%Y-%m-%d"),
            datetime.datetime(2023, 10, 16, 12, 30).strftime("%Y-%m-%d"),
            Timestamp('2023-10-15 14:30:45.123456789').strftime("%Y-%m-%d"),
            None
        ],
        'column_type': MYSQL_DATA_TYPE.DATE,
        'mysql_type': MYSQL_DATA_TYPE.DATE
    },
    {
        'input': [
            datetime.date(2023, 10, 15),
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            None
        ],
        'dtype': 'datetime64[ns]',
        'output': [
            datetime.date(2023, 10, 15).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.datetime(2023, 10, 16, 12, 30).strftime("%Y-%m-%d %H:%M:%S"),
            Timestamp('2023-10-15 14:30:45.123456789').strftime("%Y-%m-%d %H:%M:%S"),
            None
        ],
        'column_type': MYSQL_DATA_TYPE.DATETIME,
        'mysql_type': MYSQL_DATA_TYPE.DATETIME
    },
    {
        'input': [
            datetime.date(2023, 10, 15),
            datetime.datetime(2023, 10, 16, 12, 30),
            Timestamp('2023-10-15 14:30:45.123456789'),
            None
        ],
        'dtype': 'object',
        'output': [
            datetime.date(2023, 10, 15).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.datetime(2023, 10, 16, 12, 30).strftime("%Y-%m-%d %H:%M:%S"),
            Timestamp('2023-10-15 14:30:45.123456789').strftime("%Y-%m-%d %H:%M:%S"),
            None
        ],
        'column_type': MYSQL_DATA_TYPE.DATETIME,
        'mysql_type': MYSQL_DATA_TYPE.DATETIME
    },
]
column_types_tests += dt_test_cases


@pytest.mark.parametrize('test_index, test_case', enumerate(column_types_tests + dtype_tests))
def test_mysql_dump_int(test_index: int, test_case: dict):
    """Test that ResultSet.dump_to_mysql returns correct mysql types and values.

    Args:
        test_index: Index of the test case. Used only in error message to identify test case.
        test_case: Test case - input data, expected output data and output mysql type.
                   May also contain column_type to specify mysql type in input dataframe.
    """
    df = DataFrame(test_case['input'], columns=['a'], dtype=test_case['dtype'])
    mysql_types = None
    if 'column_type' in test_case:
        mysql_types = [test_case['column_type']]
    rs = ResultSet.from_df(df, mysql_types=mysql_types)
    df, columns = dump_result_set_to_mysql(rs)
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
