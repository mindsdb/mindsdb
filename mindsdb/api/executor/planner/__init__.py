from .query_planner import QueryPlanner as QueryPlanner


def plan_query(query, *args, **kwargs):
    return QueryPlanner(query, *args, **kwargs).from_query()
