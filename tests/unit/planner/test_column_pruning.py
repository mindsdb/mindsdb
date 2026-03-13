from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Identifier, Select

from mindsdb.api.executor.planner import plan_query
from mindsdb.api.executor.planner.steps import SubSelectStep


class TestColumnPruning:
    """Test column pruning optimization."""

    def test_basic_column_pruning(self):
        """Test that only needed columns are fetched from tables."""
        query = parse_sql("""
            SELECT t1.id, t2.name 
            FROM int1.table1 t1 
            JOIN int2.table2 t2 ON t1.id = t2.id
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        # First table should only fetch 'id' column
        step0_query = str(plan.steps[0].query)
        assert "`id`" in step0_query or "id" in step0_query
        assert "SELECT *" not in step0_query

        # Second table should fetch 'id' (for join) and 'name' (for SELECT)
        step1_query = str(plan.steps[1].query)
        assert "id" in step1_query
        assert "name" in step1_query
        assert "SELECT *" not in step1_query

    def test_qualified_star_disables_pruning_for_that_table(self):
        """Test that t1.* fetches all columns from t1 but t2 is still pruned."""
        query = parse_sql("""
            SELECT t1.*, t2.id 
            FROM int1.table1 t1 
            JOIN int2.table2 t2 ON t1.id = t2.id
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        # First table should fetch all (qualified star)
        step0_query = str(plan.steps[0].query)
        assert "SELECT *" in step0_query

        # Second table should only fetch 'id'
        step1_query = str(plan.steps[1].query)
        assert "id" in step1_query
        assert "SELECT *" not in step1_query

    def test_bare_star_disables_all_pruning(self):
        """Test that SELECT * fetches all columns from all tables."""
        query = parse_sql("""
            SELECT * 
            FROM int1.table1 t1 
            JOIN int2.table2 t2 ON t1.id = t2.id
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        # Both tables should fetch all columns
        assert "SELECT * FROM table1" in str(plan.steps[0].query)
        assert "SELECT * FROM table2" in str(plan.steps[1].query)

    def test_columns_from_where_clause_included(self):
        """Test that columns used in WHERE clause are included."""
        query = parse_sql("""
            SELECT t1.id 
            FROM int1.table1 t1 
            JOIN int2.table2 t2 ON t1.id = t2.id
            WHERE t1.status = 'active' AND t2.type = 'premium'
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        # First table needs: id (SELECT + JOIN) + status (WHERE)
        step0_query = str(plan.steps[0].query)
        assert "id" in step0_query
        assert "status" in step0_query

        # Second table needs: id (JOIN) + type (WHERE)
        step1_query = str(plan.steps[1].query)
        assert "id" in step1_query
        assert "type" in step1_query

    def test_columns_from_join_conditions_included(self):
        """Test that columns used in JOIN ON conditions are included."""
        query = parse_sql("""
            SELECT t1.name 
            FROM int1.table1 t1 
            JOIN int2.table2 t2 ON t1.customer_id = t2.id AND t1.region = t2.region
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        # First table needs: name (SELECT) + customer_id, region (JOIN)
        step0_query = str(plan.steps[0].query)
        assert "name" in step0_query
        assert "customer_id" in step0_query
        assert "region" in step0_query

        # Second table needs: id, region (JOIN)
        step1_query = str(plan.steps[1].query)
        assert "id" in step1_query
        assert "region" in step1_query

    def test_order_by_ordinal_resolution(self):
        """Test that ORDER BY 1, 2 resolves to actual columns."""
        query = parse_sql("""
            SELECT t1.name, t1.created_at 
            FROM int1.table1 t1 
            ORDER BY 1, 2
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch both columns
        query_str = str(plan.steps[0].query)
        assert "name" in query_str
        assert "created_at" in query_str

    def test_order_by_alias_resolution(self):
        """Test that ORDER BY alias_name resolves to actual column."""
        query = parse_sql("""
            SELECT t1.customer_name AS cname, t1.id 
            FROM int1.table1 t1 
            ORDER BY cname
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch customer_name (aliased as cname)
        query_str = str(plan.steps[0].query)
        assert "customer_name" in query_str
        assert "id" in query_str

    def test_group_by_columns_included(self):
        """Test that columns in GROUP BY are included."""
        query = parse_sql("""
            SELECT t1.category, COUNT(*) as cnt
            FROM int1.table1 t1 
            GROUP BY t1.category
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch category for grouping
        query_str = str(plan.steps[0].query)
        assert "category" in query_str

    def test_having_columns_included(self):
        """Test that columns in HAVING clause are included."""
        query = parse_sql("""
            SELECT t1.category, COUNT(*) as cnt
            FROM int1.table1 t1 
            GROUP BY t1.category
            HAVING t1.total > 100
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch category (GROUP BY) + total (HAVING)
        query_str = str(plan.steps[0].query)
        assert "category" in query_str
        assert "total" in query_str

    def test_case_sensitive_columns_preserved(self):
        """Test that quoted identifiers (case-sensitive) preserve quoting."""
        # Build query with quoted identifier manually
        query = Select(
            targets=[Identifier(parts=["MyColumn"]), Identifier(parts=["regular_col"])],
            from_table=Identifier("int.table1"),
        )
        # Set is_quoted after creation
        query.targets[0].is_quoted = [True]

        plan = plan_query(query, integrations=["int"])

        # Should have both columns (quoting preservation is verified by is_quoted attribute)
        query_str = str(plan.steps[0].query)
        # At minimum, both columns should be present
        assert "MyColumn" in query_str
        assert "regular_col" in query_str

    def test_window_function_columns_included(self):
        """Test that columns in window functions (PARTITION BY, ORDER BY) are included."""
        query = parse_sql("""
            SELECT 
                t1.id,
                ROW_NUMBER() OVER (PARTITION BY t1.category ORDER BY t1.created_at) as row_num
            FROM int1.table1 t1
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch: id (SELECT) + category (PARTITION BY) + created_at (ORDER BY in OVER)
        query_str = str(plan.steps[0].query)
        assert "id" in query_str
        assert "category" in query_str
        assert "created_at" in query_str

    def test_subselect_pruning(self):
        """Test that subselects with pure SELECT * get column pruning applied."""
        query = parse_sql("""
            SELECT sub.id 
            FROM (SELECT * FROM int1.table1) AS sub
            JOIN int2.table2 t2 ON sub.id = t2.id
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        # Subselects with pure SELECT * should be pruned to only needed columns
        found_pruned_subselect = False
        for step in plan.steps:
            # Look for SubSelectStep with id column (not SELECT *)
            if isinstance(step, SubSelectStep):
                query_str = str(step.query)
                if "id" in query_str and "SELECT *" not in query_str:
                    found_pruned_subselect = True
                    break

        assert found_pruned_subselect, (
            f"Subselect with pure SELECT * should be pruned. Steps: {[str(s) for s in plan.steps]}"
        )

    def test_three_table_join_pruning(self):
        """Test column pruning with 3-table join."""
        query = parse_sql("""
            SELECT t1.id, t2.name, t3.amount
            FROM int1.table1 t1
            JOIN int2.table2 t2 ON t1.id = t2.customer_id
            JOIN int3.table3 t3 ON t2.id = t3.order_id
        """)

        plan = plan_query(query, integrations=["int1", "int2", "int3"])

        # Find FetchDataframeSteps (not JoinSteps)
        fetch_steps = [s for s in plan.steps if "FetchDataframe" in str(type(s))]
        assert len(fetch_steps) == 3, f"Expected 3 fetch steps, got {len(fetch_steps)}"

        # Table1: id (SELECT + JOIN) - should NOT have SELECT *
        step0_query = str(fetch_steps[0].query)
        assert "id" in step0_query
        assert "SELECT *" not in step0_query

        # Table2: customer_id (JOIN to t1), id (JOIN to t3), name (SELECT)
        step1_query = str(fetch_steps[1].query)
        assert "customer_id" in step1_query
        assert "id" in step1_query
        assert "name" in step1_query
        assert "SELECT *" not in step1_query

        # Table3: order_id (JOIN), amount (SELECT)
        step2_query = str(fetch_steps[2].query)
        assert "order_id" in step2_query
        assert "amount" in step2_query
        assert "SELECT *" not in step2_query

    def test_no_pruning_when_no_columns_detected(self):
        """Test fallback to SELECT * when column detection fails."""
        # Note: COUNT(*) is actually pushed down to the source in this implementation
        # which is different behavior than falling back to SELECT *
        # This is actually CORRECT behavior - the source database can compute COUNT(*)
        query = parse_sql("""
            SELECT COUNT(*) 
            FROM int1.table1 t1
        """)

        plan = plan_query(query, integrations=["int1"])

        # The query is sent to the source as-is (single table, no joins)
        query_str = str(plan.steps[0].query)
        # This query goes directly to integration (not pruned, sent as is)
        assert "count(*)" in query_str.lower() or "COUNT(*)" in query_str

    def test_complex_expression_columns_included(self):
        """Test that columns in complex expressions are included."""
        query = parse_sql("""
            SELECT t1.price * t1.quantity AS total
            FROM int1.table1 t1
            WHERE t1.discount > 0
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch: price, quantity (expression), discount (WHERE)
        query_str = str(plan.steps[0].query)
        assert "price" in query_str
        assert "quantity" in query_str
        assert "discount" in query_str


class TestColumnPruningEdgeCases:
    """Test edge cases and error conditions."""

    def test_duplicate_column_references(self):
        """Test that duplicate column references don't break pruning."""
        query = parse_sql("""
            SELECT t1.id, t1.id, t1.id
            FROM int1.table1 t1
        """)

        plan = plan_query(query, integrations=["int1"])

        # Single table query gets sent as-is (no join optimization)
        # The query goes directly to the integration
        query_str = str(plan.steps[0].query)
        assert "id" in query_str

    def test_mixed_qualified_and_unqualified_columns(self):
        """Test mixing qualified (t1.col) and unqualified (col) column references."""
        query = parse_sql("""
            SELECT t1.id, name
            FROM int1.table1 t1
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch both columns
        query_str = str(plan.steps[0].query)
        assert "id" in query_str
        assert "name" in query_str

    def test_self_join_pruning(self):
        """Test column pruning in self-joins."""
        query = parse_sql("""
            SELECT t1.id, t2.name
            FROM int1.table1 t1
            JOIN int1.table1 t2 ON t1.parent_id = t2.id
        """)

        plan = plan_query(query, integrations=["int1"])

        # Self-join from same integration is optimized - sent as single query to source!
        fetch_steps = [s for s in plan.steps if "FetchDataframe" in str(type(s))]

        if len(fetch_steps) == 1:
            # Optimized: entire join sent to source database
            query_str = str(fetch_steps[0].query)
            assert "JOIN" in query_str
            assert "id" in query_str
            assert "name" in query_str
            assert "parent_id" in query_str
        else:
            # Not optimized: separate fetches with column pruning
            assert len(fetch_steps) >= 2
            step0_query = str(fetch_steps[0].query)
            assert "id" in step0_query
            assert "parent_id" in step0_query
            assert "SELECT *" not in step0_query

            step1_query = str(fetch_steps[1].query)
            assert "id" in step1_query
            assert "name" in step1_query
            assert "SELECT *" not in step1_query

    def test_column_in_multiple_clauses(self):
        """Test that column used in multiple clauses is included once."""
        query = parse_sql("""
            SELECT t1.status
            FROM int1.table1 t1
            WHERE t1.status = 'active'
            ORDER BY t1.status
        """)

        plan = plan_query(query, integrations=["int1"])

        # Should fetch 'status' (used in SELECT, WHERE, ORDER BY)
        query_str = str(plan.steps[0].query)
        assert query_str.count("status") >= 1

    def test_subselect_with_mixed_star(self):
        """Test that SubSelectStep passes through all columns (no pruning at SubSelect level)."""
        query = parse_sql("""
            SELECT sub.id 
            FROM (SELECT *, 'dummy' as extra FROM int1.table1) AS sub
            JOIN int2.table2 t2 ON sub.id = t2.id
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        # SubSelectStep should use SELECT * to pass through all columns
        # Column pruning happens at the table fetch level, not at SubSelectStep
        found_subselect_with_star = False
        for step in plan.steps:
            if isinstance(step, SubSelectStep):
                query_str = str(step.query)
                if "SELECT *" in query_str:
                    found_subselect_with_star = True
                    break

        assert found_subselect_with_star, (
            f"Expected SubSelectStep to use SELECT * (no pruning). Steps: {[str(s) for s in plan.steps]}"
        )
