from tests.load.tasks import BaseDBConnectionBehavior


class PostgreSQLConnectionBehavior(BaseDBConnectionBehavior):
    """
    This class defines the behavior of a PostgreSQL connection.
    @TODO: Read query values from sql_queries.json file
    """
    db_type = "postgres"
    table_name = "solar_flare_data"
    native_queries = ["native_query_average", "native_query_aggregation", "native_query_max", "native_query_grouping"]
    native_query_aggregation = f"SELECT COUNT(*) AS total_flares FROM tests.{table_name};"
    native_query_average = f"SELECT AVG(peak_c_per_s) AS avg_peak_counts FROM tests.{table_name};"
    native_query_max = f"SELECT MAX(energy_kev) AS max_energy FROM tests.{table_name};"
    native_query_grouping = f"SELECT active_region_ar, COUNT(*) AS flare_count  FROM tests.{table_name} GROUP BY active_region_ar;"
