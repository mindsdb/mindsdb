import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor


class OpenStreetMapNodeTable(APITable):
    """The OpenStreetMap Nodes Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the OpenStreetMap API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            OpenStreetMap data matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        where_conditions = extract_comparison_conditions(query.where)

        if query.limit:
            result_limit = query.limit.value
        else:
            result_limit = 20

        nodes_df = pd.json_normalize(self.get_nodes(where_conditions=where_conditions, limit=result_limit))

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = nodes_df.columns
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        order_by_conditions = {}
        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] == 'nodes':
                    if an_order.field.parts[1] in nodes_df.columns:
                        order_by_conditions["columns"].append(an_order.field.parts[1])

                        if an_order.direction == "ASC":
                            order_by_conditions["ascending"].append(True)
                        else:
                            order_by_conditions["ascending"].append(False)
                    else:
                        raise ValueError(
                            f"Order by unknown column {an_order.field.parts[1]}"
                        )

        select_statement_executor = SELECTQueryExecutor(
            nodes_df,
            selected_columns,
            [],
            order_by_conditions
        )
        nodes_df = select_statement_executor.execute_query()

        return nodes_df

    def get_nodes(self, **kwargs) -> List[Dict]:
        where_conditions = kwargs.get('where_conditions', None)

        area, tags = None, {}
        min_lat, min_lon, max_lat, max_lon = None, None, None, None
        if where_conditions:
            for condition in where_conditions:
                if condition[1] == 'area':
                    area = condition[2]

                elif condition[1] == 'min_lat':
                    min_lat = condition[2]

                elif condition[1] == 'min_lon':
                    min_lon = condition[2]

                elif condition[1] == 'max_lat':
                    max_lat = condition[2]

                elif condition[1] == 'max_lon':
                    max_lon = condition[2]

                else:
                    tags[condition[1]] = condition[2]

        result = self.execute_osm_node_query(
            tags=tags,
            area=area,
            min_lat=min_lat,
            min_lon=min_lon,
            max_lat=max_lat,
            max_lon=max_lon,
            limit=kwargs.get('limit', None)
        )

        nodes = []
        for node in result.nodes:
            node_dict = {
                "id": node.id,
                "lat": node.lat,
                "lon": node.lon,
                "tags": node.tags
            }
            nodes.append(node_dict)
        return nodes

    def execute_osm_node_query(self, tags, area=None, min_lat=None, min_lon=None, max_lat=None, max_lon=None, limit=None):
        query_template = """
        [out:json];
        {area_clause}
        node{area_node_clause}{tags_clause}{bbox};
        out {limit};
        """

        tags_clause = ""
        if tags:
            for tag_key, tag_value in tags.items():
                tags_clause += '["{}"="{}"]'.format(tag_key, tag_value)

        area_clause, area_node_clause = "", ""
        if area:
            area_clause = 'area[name="{}"]->.city;\n'.format(area)
            area_node_clause = "(area.city)"

        bbox_clause = ""
        if min_lat or min_lon or max_lat or max_lon:
            bbox_clause = "{},{},{},{}".format(min_lat, min_lon, max_lat, max_lon)

        limit_clause = limit if limit else ""

        query = query_template.format(
            area_clause=area_clause,
            area_node_clause=area_node_clause,
            tags_clause=tags_clause,
            bbox=bbox_clause,
            limit=limit_clause
        )

        api = self.handler.connect()

        result = api.query(query)
        return result


class OpenStreetMapWayTable(APITable):
    """The OpenStreetMap Ways Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:

        select_statement_parser = SELECTQueryParser(
            query,
            'ways',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        ways_df = pd.json_normalize(self.get_ways(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            ways_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        ways_df = select_statement_executor.execute_query()

        return ways_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_ways(limit=1)).columns.tolist()

    def get_ways(self, **kwargs) -> List[Dict]:

        api_session = self.handler.connect()
        ways = api_session.query("""
            way
            ({{bbox}});
            out;
            """,
                                 # bbox=self.connection_data['bbox']
                                 )
        return [way.to_dict() for way in ways.ways]


class OpenStreetMapRelationTable(APITable):
    """The OpenStreetMap Relations Table implementation"""

    def select_relations(self, query: ast.Select) -> pd.DataFrame:

        select_statement_parser = SELECTQueryParser(
            query,
            'relations',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        relations_df = pd.json_normalize(self.get_relations(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            relations_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        relations_df = select_statement_executor.execute_query()

        return relations_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_relations(limit=1)).columns.tolist()

    def get_relations(self, **kwargs) -> List[Dict]:

        api_session = self.handler.connect()
        relations = api_session.query("""
                    relation
                    ({{bbox}});
                    out;
                    """,
                                      # bbox=self.connection_data['bbox']
                                      )
        return [relation.to_dict() for relation in relations.relations]
