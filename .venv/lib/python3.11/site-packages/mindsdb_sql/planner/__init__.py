from .query_planner import QueryPlanner


def plan_query(query, *args, **kwargs):
    return QueryPlanner(query, *args, **kwargs).from_query()

