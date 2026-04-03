"""
Regression tests for LLM-generated SQL identifier normalization.
See: https://github.com/mindsdb/mindsdb/issues/11156
"""
from mindsdb.interfaces.agents.utils.sql_toolkit import normalize_kb_table_ref
from mindsdb_sql import parse_sql


class TestNormalizeKbTableRef:

    def test_backtick_wrapped_qualified_name(self):
        # Exact pattern from issue #11156
        sql = "SELECT * FROM `mindsdb.2506_00075_kb` WHERE content = 'methodology'"
        result = normalize_kb_table_ref(sql)
        assert '`mindsdb.2506_00075_kb`' not in result
        assert 'mindsdb.`2506_00075_kb`' in result

    def test_qualified_name_with_hyphens_in_kb(self):
        sql = "SELECT * FROM `mindsdb.my-kb` WHERE content = 'x'"
        result = normalize_kb_table_ref(sql)
        assert '`mindsdb.my-kb`' not in result
        assert 'mindsdb.`my-kb`' in result

    def test_three_part_qualified_name(self):
        sql = "SELECT * FROM `db.schema.table`"
        result = normalize_kb_table_ref(sql)
        assert result == "SELECT * FROM db.schema.table"
        
        sql_with_hyphen = "SELECT * FROM `db.schema.my-table`"
        result_with_hyphen = normalize_kb_table_ref(sql_with_hyphen)
        assert result_with_hyphen == "SELECT * FROM db.schema.`my-table`"

    def test_plain_quoted_single_identifier_preserved(self):
        # Single identifier that needs quoting should stay quoted
        sql = "SELECT * FROM `my-kb` WHERE content = 'x'"
        result = normalize_kb_table_ref(sql)
        assert '`my-kb`' in result

    def test_clean_single_identifier_unquoted(self):
        sql = "SELECT * FROM `my_kb` WHERE content = 'x'"
        result = normalize_kb_table_ref(sql)
        assert result == "SELECT * FROM my_kb WHERE content = 'x'"

    def test_already_correct_sql_unchanged(self):
        sql = "SELECT * FROM mindsdb.test_kb WHERE content = 'x'"
        result = normalize_kb_table_ref(sql)
        assert result == sql

    def test_multiple_references_in_one_query(self):
        sql = "SELECT * FROM `mindsdb.kb_a` JOIN `mindsdb.kb_b` ON id"
        result = normalize_kb_table_ref(sql)
        assert '`mindsdb.kb_a`' not in result
        assert '`mindsdb.kb_b`' not in result
        assert 'mindsdb.kb_a' in result
        assert 'mindsdb.kb_b' in result

    def test_backticks_in_other_clauses_preserved(self):
        # Backticks in SELECT (column names/aliases) or WHERE (string literals) must not be corrupted
        sql = "SELECT `my.col` FROM mindsdb.test_kb WHERE content = '`some.value`'"
        result = normalize_kb_table_ref(sql)
        assert result == sql

    def test_ast_equivalence_after_normalization(self):
        bad = "SELECT * FROM `mindsdb.test_kb` WHERE content = 'x'"
        good = "SELECT * FROM mindsdb.test_kb WHERE content = 'x'"
        
        normalized_bad = normalize_kb_table_ref(bad)
        
        ast_from_bad = parse_sql(normalized_bad, dialect='mindsdb')
        ast_from_good = parse_sql(good, dialect='mindsdb')
        
        # AST string representations should match completely
        assert ast_from_bad.to_string() == ast_from_good.to_string()
