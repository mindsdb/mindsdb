import os
import json

import psycopg

from mindsdb.utilities import log

logger = log.getLogger(__name__)


MINDSDB_PROFILING_ENABLED = os.environ.get("MINDSDB_PROFILING_ENABLED") in ("1", "true")
MINDSDB_PROFILING_DB_HOST = os.environ.get("MINDSDB_PROFILING_DB_HOST")
MINDSDB_PROFILING_DB_USER = os.environ.get("MINDSDB_PROFILING_DB_USER")
MINDSDB_PROFILING_DB_PASSWORD = os.environ.get("MINDSDB_PROFILING_DB_PASSWORD")


def set_level(node, level, internal_id):
    internal_id["id"] += 1
    node["level"] = level
    node["value"] = node["stop_at"] - node["start_at"]
    node["value_thread"] = node["stop_at_thread"] - node["start_at_thread"]
    node["value_process"] = node["stop_at_process"] - node["start_at_process"]
    node["internal_id"] = internal_id["id"]

    accum = 0
    for child_node in node["children"]:
        set_level(child_node, level + 1, internal_id)
        accum += child_node["value"]
    node["self"] = node["value"] - accum


def send_profiling_results(profiling_data: dict):
    if MINDSDB_PROFILING_ENABLED is False:
        return

    profiling = profiling_data
    set_level(profiling["tree"], 0, {"id": 0})

    time_start_at = profiling["tree"]["time_start_at"]
    del profiling["tree"]["time_start_at"]

    try:
        connection = psycopg.connect(
            host=MINDSDB_PROFILING_DB_HOST,
            port=5432,
            user=MINDSDB_PROFILING_DB_USER,
            password=MINDSDB_PROFILING_DB_PASSWORD,
            dbname="postgres",
            connect_timeout=5
        )
    except Exception:
        logger.error('cant get acceess to profiling database')
        return
    cur = connection.cursor()
    cur.execute("""
        insert into profiling
            (data, query, time, hostname, environment, api, total_time, company_id, instance_id)
        values
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        json.dumps(profiling["tree"]),
        profiling.get("query", "?"),
        time_start_at,
        profiling["hostname"],
        profiling.get("environment", "?"),
        profiling.get("api", "?"),
        profiling["tree"]["value"],
        profiling["company_id"],
        profiling["instance_id"]
    ))

    connection.commit()
    cur.close()
    connection.close()
