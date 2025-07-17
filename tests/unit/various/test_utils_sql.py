import pytest

from mindsdb.utilities.sql import clear_sql

test_cases = [
    [
        '--comment\nselect a, /*comment*/ from b; # comment\n',
        'select a,  from b'
    ], [
        '#comment\nselect a, /*comment*/ from b; -- comment\n',
        'select a,  from b'
    ], [
        'select "--comment\n", "/*comment*/" from b "#comment"/*comment*/;',
        'select "--comment\n", "/*comment*/" from b "#comment"'
    ], [
        'select `--comment\t`, `/*comment*/` from b `#comment`--comment\n;',
        'select `--comment\t`, `/*comment*/` from b `#comment`'
    ], [
        "select '--comment\n', '/*comment*/' from b '#comment'#comment\n;",
        "select '--comment\n', '/*comment*/' from b '#comment'"
    ], [
        "select `'`--comment\n`'`, '/*comment*/' from b '#comment'#comment\n;",
        "select `'`--comment\n`'`, '/*comment*/' from b '#comment'"
    ]
]


@pytest.mark.parametrize('test_index, test_case', enumerate(test_cases))
def test_utils_sql(test_index, test_case):
    input_sql, expected_sql = test_case
    result = clear_sql(input_sql)
    assert result == expected_sql, f"Test case {test_index} failed: {result} != {expected_sql}"
