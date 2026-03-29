import datetime as dt
from typing import Dict, Union, Any, Optional, Tuple as TypingTuple

from bson.objectid import ObjectId
from mindsdb_sql_parser.ast import (
    Select,
    Update,
    Identifier,
    Star,
    Constant,
    Tuple,
    BinaryOperation,
    Latest,
    TypeCast,
    Function,
)
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_query import MongoQuery


# TODO: Create base NonRelationalRender as SqlAlchemyRender
class NonRelationalRender:
    pass


class MongodbRender(NonRelationalRender):
    """
    Renderer to convert SQL queries represented as ASTNodes to MongoQuery instances.
    """

    def _parse_inner_query(self, node: ASTNode) -> Dict[str, Any]:
        """
        Return field ref like "$field" or constant for projection expressions.
        """
        if isinstance(node, Identifier):
            return f"${node.parts[-1]}"
        elif isinstance(node, Constant):
            return node.value
        else:
            raise NotImplementedError(f"Not supported inner query {node}")

    def _convert_type_cast(self, node: TypeCast) -> Dict[str, Any]:
        """
        Converts a TypeCast ASTNode to a MongoDB-compatible format.

        Args:
            node (TypeCast): The TypeCast node to be converted.

        Returns:
            Dict[str, Any]: The converted type cast representation.
        """
        inner_query = self._parse_inner_query(node.arg)
        type_name = node.type_name.upper()

        def convert(value: Any, to_type: str) -> Dict[str, Any]:
            return {"$convert": {"input": value, "to": to_type, "onError": None}}

        if type_name in ("VARCHAR", "TEXT", "STRING"):
            return convert(inner_query, "string")

        if type_name in ("INT", "INTEGER", "BIGINT", "LONG"):
            return convert(inner_query, "long")

        if type_name in ("DOUBLE", "FLOAT", "DECIMAL", "NUMERIC"):
            return convert(inner_query, "double")

        if type_name in ("DATE", "DATETIME", "TIMESTAMP"):
            return convert(inner_query, "date")

        return inner_query

    def _parse_select(self, from_table: Any) -> TypingTuple[str, Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Parses the from_table to extract the collection name
        If from_table is subquery, transform it for MongoDB
        Args:
            from_table (Any): The from_table to be parsed.
        Returns:
            str: The collection name.
            Dict[str, Any]: The query filters.
            Optional[Dict[str, Any]]: The projection fields.
        """
        # Simple collection
        if isinstance(from_table, Identifier):
            return from_table.parts[-1], {}, None

        # Trivial subselect
        if isinstance(from_table, Select):
            # reject complex forms early
            # how deep we want to go with subqueries?
            if from_table.group_by is not None or from_table.having is not None:
                raise NotImplementedError(f"Not supported, subquery has `having` or `group by`: {from_table}")

            if not isinstance(from_table.from_table, Identifier):
                raise NotImplementedError(f"Only simple subqueries are allowed in {from_table}")

            collection = from_table.from_table.parts[-1]

            pre_match: Dict[str, Any] = {}
            if from_table.where is not None:
                pre_match = self.handle_where(from_table.where)

            if from_table.targets is None:
                pre_project: Optional[Dict[str, Any]] = {"_id": 0}
            else:
                saw_star = any(isinstance(t, Star) for t in from_table.targets)
                if saw_star:
                    pre_project = None
                else:
                    pre_project = {"_id": 0}
                    for t in from_table.targets:
                        if isinstance(t, Identifier):
                            name = ".".join(t.parts)
                            alias = name if t.alias is None else t.alias.parts[-1]
                            pre_project[alias] = f"${name}"
                        elif isinstance(t, Constant):
                            alias = str(t.value) if t.alias is None else t.alias.parts[-1]
                            pre_project[alias] = t.value
                        elif isinstance(t, TypeCast):
                            alias = t.alias.parts[-1] if t.alias is not None else t.arg.parts[-1]
                            pre_project[alias] = self._convert_type_cast(t)
                        else:
                            raise NotImplementedError(f"Unsupported inner target: {t}")

            return collection, pre_match, pre_project

        raise NotImplementedError(f"Not supported from {from_table}")

    def to_mongo_query(self, node: ASTNode) -> MongoQuery:
        """
        Converts SQL query to MongoQuery instance.

        Args:
            node (ASTNode): An ASTNode representing the SQL query to be converted.

        Returns:
            MongoQuery: The converted MongoQuery instance.
        """
        if isinstance(node, Select):
            return self.select(node)
        elif isinstance(node, Update):
            return self.update(node)
        raise NotImplementedError(f"Unknown statement: {node.__class__.__name__}")

    def update(self, node: Update) -> MongoQuery:
        """
        Converts an Update statement to MongoQuery instance.

        Args:
            node (Update): An ASTNode representing the SQL Update statement.

        Returns:
            MongoQuery: The converted MongoQuery instance.
        """
        collection = node.table.parts[-1]
        mquery = MongoQuery(collection)

        filters = self.handle_where(node.where)
        row = {k: v.value for k, v in node.update_columns.items()}
        mquery.add_step({"method": "update_many", "args": [filters, {"$set": row}]})
        return mquery

    def select(self, node: Select) -> MongoQuery:
        """
        Converts a Select statement to MongoQuery instance.

        Args:
            node (Select): An ASTNode representing the SQL Select statement.

        Returns:
            MongoQuery: The converted MongoQuery instance.
        """
        collection, pre_match, pre_project = self._parse_select(node.from_table)

        # check for table aliases
        table_aliases = {collection}

        if isinstance(node.from_table, Identifier) and node.from_table.alias is not None:
            table_aliases.add(node.from_table.alias.parts[-1])

        filters: Dict[str, Any] = {}
        agg_group: Dict[str, Any] = {}

        if node.where is not None:
            filters = self.handle_where(node.where)

        group: Dict[str, Any] = {}
        project: Dict[str, Any] = {"_id": 0}
        if node.distinct:
            # Group by distinct fields.
            group = {"_id": {}}

        if node.targets is not None:
            for col in node.targets:
                if isinstance(col, Star):
                    # Show all fields.
                    project = {}
                    break
                if isinstance(col, Identifier):
                    parts = list(col.parts)

                    # Strip table alias/qualifier prefix if present
                    if len(parts) > 1:
                        if parts[0] in table_aliases:
                            parts = parts[1:]
                        elif len(table_aliases) == 1:
                            parts = parts[1:]

                    # Convert parts to strings and join
                    name = ".".join(str(p) for p in parts) if len(parts) > 0 else str(parts[0])

                    if col.alias is None:
                        alias = name
                    else:
                        alias = col.alias.parts[-1]

                    project[alias] = f"${name}"  # Project field.

                    # Group by distinct fields.
                    if node.distinct:
                        group["_id"][name] = f"${name}"  # Group field.
                        group[name] = {"$first": f"${name}"}  # Show field.

                elif isinstance(col, Function):
                    func_name = col.op.lower()
                    alias = col.alias.parts[-1] if col.alias is not None else func_name

                    if len(col.args) == 0:
                        raise NotImplementedError(f"Function {func_name.upper()} requires arguments")

                    arg0 = col.args[0]

                    if func_name == "count" and isinstance(arg0, Star):
                        agg_group[alias] = {"$sum": 1}
                    elif isinstance(arg0, Identifier):
                        args_parts = list(arg0.parts)

                        # Strip table alias/qualifier prefix if present
                        if len(args_parts) > 1:
                            if args_parts[0] in table_aliases:
                                args_parts = args_parts[1:]
                            # Handle implicit qualifiers in single-table queries
                            elif len(table_aliases) == 1:
                                args_parts = args_parts[1:]

                        # Convert parts to strings and join
                        field_name = ".".join(str(p) for p in args_parts) if len(args_parts) > 0 else str(args_parts[0])

                        if func_name == "avg":
                            agg_group[alias] = {"$avg": f"${field_name}"}
                        elif func_name == "sum":
                            agg_group[alias] = {"$sum": f"${field_name}"}
                        elif func_name == "count":
                            agg_group[alias] = {"$sum": {"$cond": [{"$ne": [f"${field_name}", None]}, 1, 0]}}
                        elif func_name == "min":
                            agg_group[alias] = {"$min": f"${field_name}"}
                        elif func_name == "max":
                            agg_group[alias] = {"$max": f"${field_name}"}
                        else:
                            raise NotImplementedError(f"Aggregation function '{func_name.upper()}' is not supported")
                elif isinstance(col, Constant):
                    alias = str(col.value) if col.alias is None else col.alias.parts[-1]
                    project[alias] = col.value

        if node.group_by is not None:
            if "_id" not in group or not isinstance(group["_id"], dict):
                group["_id"] = {}

            for group_col in node.group_by:
                if not isinstance(group_col, Identifier):
                    raise NotImplementedError(f"Unsupported GROUP BY column {group_col}")
                group_parts = list(group_col.parts)

                # Strip table alias/qualifier prefix if present
                if len(group_parts) > 1:
                    if group_parts[0] in table_aliases:
                        group_parts = group_parts[1:]
                    # Handle implicit qualifiers in single-table queries
                    elif len(table_aliases) == 1:
                        group_parts = group_parts[1:]

                # Convert parts to strings and join
                field_name = ".".join(str(p) for p in group_parts)
                alias = group_col.alias.parts[-1] if group_col.alias is not None else field_name

                group["_id"][alias] = f"${field_name}"

                if alias in project:
                    group[alias] = {"$first": f"${field_name}"}
                    project[alias] = f"${alias}"

            for alias, expression in agg_group.items():
                group[alias] = expression
                project[alias] = f"${alias}"
        elif agg_group:
            group = {"_id": None}
            for alias, expression in agg_group.items():
                group[alias] = expression
                project[alias] = f"${alias}"

        sort = {}
        if node.order_by is not None:
            for col in node.order_by:
                name = col.field.parts[-1]
                direction = 1 if col.direction.upper() == "ASC" else -1
                sort[name] = direction

        # Compose the MongoDB query.
        mquery = MongoQuery(collection)

        method: str = "aggregate"
        margs: list = []

        # MongoDB related pipeline steps for the aggregate method.
        if node.modifiers is not None:
            for modifier in node.modifiers:
                margs.append(modifier)

        if pre_match:
            margs.append({"$match": pre_match})
        if pre_project is not None and pre_project != {}:
            margs.append({"$project": pre_project})

        if filters:
            margs.append({"$match": filters})

        if group:
            margs.append({"$group": group})

        if project:
            margs.append({"$project": project})

        if sort:
            margs.append({"$sort": sort})

        if node.offset is not None:
            margs.append({"$skip": int(node.offset.value)})

        if node.limit is not None:
            margs.append({"$limit": int(node.limit.value)})

        mquery.add_step({"method": method, "args": [margs]})

        return mquery

    def handle_where(self, node: BinaryOperation) -> Dict:
        """
        Converts a BinaryOperation node to a dictionary of MongoDB query filters.

        Args:
            node (BinaryOperation): A BinaryOperation node representing the SQL WHERE clause.

        Returns:
            dict: The converted MongoDB query filters.
        """
        # TODO: UnaryOperation, function.
        if not isinstance(node, BinaryOperation):
            raise NotImplementedError(f"Not supported type {type(node)}")

        op = node.op.lower()
        a, b = node.args

        if op in ("and", "or"):
            left = self.handle_where(a)
            right = self.handle_where(b)
            ops = {
                "and": "$and",
                "or": "$or",
            }
            query = {ops[op]: [left, right]}
            return query

        ops_map = {
            ">=": "$gte",
            ">": "$gt",
            "<": "$lt",
            "<=": "$lte",
            "<>": "$ne",
            "!=": "$ne",
            "=": "$eq",
            "==": "$eq",
            "is": "$eq",
            "is not": "$ne",
        }

        if isinstance(a, Identifier):
            var_name = ".".join(a.parts)
            # Simple operation.
            if isinstance(b, Constant):
                # Identifier and Constant.
                val = ObjectId(b.value) if var_name == "_id" else b.value
                if op in ("=", "=="):
                    pass
                elif op in ops_map:
                    op2 = ops_map[op]
                    val = {op2: val}
                else:
                    raise NotImplementedError(f"Not supported operator {op}")

                return {var_name: val}

            # IN condition.
            elif isinstance(b, Tuple):
                # Should be IN, NOT IN.
                ops = {"in": "$in", "not in": "$nin"}
                # Must be list of Constants.
                values = [i.value for i in b.items]
                if op in ops:
                    op2 = ops[op]
                    cond = {op2: values}
                else:
                    raise NotImplementedError(f"Not supported operator {op}")

                return {var_name: cond}

        # Create expression.
        val1 = self.where_element_convert(a)
        val2 = self.where_element_convert(b)

        if op in ops_map:
            op2 = ops_map[op]
        else:
            raise NotImplementedError(f"Not supported operator {op}")

        return {"$expr": {op2: [val1, val2]}}

    def where_element_convert(self, node: Union[Identifier, Latest, Constant, TypeCast]) -> Any:
        """
        Converts a WHERE element to the corresponding MongoDB query element.

        Args:
            node (Union[Identifier, Latest, Constant, TypeCast]): The WHERE element to be converted.

        Returns:
            Any: The converted MongoDB query element.

        Raises:
            NotImplementedError: If the WHERE element is not supported.
            RuntimeError: If the date format is not supported.
        """
        if isinstance(node, Identifier):
            return f"${'.'.join(node.parts)}"
        elif isinstance(node, Latest):
            return "LATEST"
        elif isinstance(node, Constant):
            return node.value
        elif isinstance(node, TypeCast) and node.type_name.upper() in (
            "DATE",
            "DATETIME",
        ):
            formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f"]
            for format in formats:
                try:
                    return dt.datetime.strptime(node.arg.value, format)
                except ValueError:
                    pass
            raise RuntimeError(f"Not supported date format. Supported: {formats}")
        else:
            raise NotImplementedError(f"Unknown where element {node}")
