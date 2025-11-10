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
from sympy import arg

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

        if type_name in ("VARCHAR", "TEXT", "STRING"):
            return {"$convert": {"input": inner_query, "to": "string", "onError": None}}

        if type_name in ("INT", "INTEGER", "BIGINT", "LONG"):
            return {"$convert": {"input": inner_query, "to": "long", "onError": None}}

        if type_name in ("DOUBLE", "FLOAT", "DECIMAL", "NUMERIC"):
            return {"$convert": {"input": inner_query, "to": "double", "onError": None}}

        if type_name in ("DATE", "DATETIME", "TIMESTAMP"):
            return {"$convert": {"input": inner_query, "to": "date", "onError": None}}

        return inner_query

    def _parse_select(
        self, from_table: Any
    ) -> TypingTuple[str, Dict[str, Any], Optional[Dict[str, Any]]]:
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
            if from_table.group_by is not None or from_table.having is not None:
                raise NotImplementedError(f"Not supported FROM as {from_table}")

            if not isinstance(from_table.from_table, Identifier):
                raise NotImplementedError(f"Not supported FROM as {from_table}")

            collection = from_table.from_table.parts[-1]

            pre_match: Dict[str, Any] = {}
            if from_table.where is not None:
                pre_match = self.handle_where(from_table.where)

            pre_project: Optional[Dict[str, Any]] = {"_id": 0}
            if from_table.targets is not None:
                saw_star = False
                for t in from_table.targets:
                    if isinstance(t, Star):
                        saw_star = True
                        break
                    if isinstance(t, Identifier):
                        name = ".".join(t.parts)
                        alias = name if t.alias is None else t.alias.parts[-1]
                        pre_project[alias] = f"${name}"
                    elif isinstance(t, Constant):
                        val = str(t.value)
                        alias = val if t.alias is None else t.alias.parts[-1]
                        pre_project[alias] = val
                    elif isinstance(t, TypeCast):
                        alias = (
                            t.alias.parts[-1]
                            if t.alias is not None
                            else t.arg.parts[-1]
                        )
                        pre_project[alias] = self._convert_type_cast(t)
                    else:
                        raise NotImplementedError(f"Unsupported inner target: {t}")
                if saw_star:
                    pre_project = {}
            else:
                pre_project = {"_id": 0}

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

    def select(self, node: Select):
        """
        Converts a Select statement to MongoQuery instance.

        Args:
            node (Select): An ASTNode representing the SQL Select statement.

        Returns:
            MongoQuery: The converted MongoQuery instance.
        """
        # if not isinstance(node.from_table, Identifier):
        #     raise NotImplementedError(f"Not supported from {node.from_table}")
        collection, pre_match, pre_project = self._parse_select(node.from_table)

        filters: Dict[str, Any] = {}
        agg_group: Dict[str, Any] = {}

        if node.where is not None:
            filters = self.handle_where(node.where)

        group: Dict[str, Any] = {}
        project = {
            "_id": 0
        }  # Hide _id field when it has not been explicitly requested.
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
                    name = ".".join(col.parts)
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

                    func_name = col.name.lower()
                    alias = col.alias.parts[-1] if col.alias is not None else func_name
                    if len(col.args) > 0 and isinstance(col.args[0], Identifier):
                        field_name = ".".join(col.args[0].parts)

                        # Map SQL functions to MongoDB operators
                        if func_name == "avg":
                            agg_group[alias] = {"$avg": f"${field_name}"}
                        elif func_name == "sum":
                            agg_group[alias] = {"$sum": f"${field_name}"}
                        elif func_name == "count":
                            if isinstance(col.args[0], Star):
                                agg_group[alias] = {"$sum": 1}
                            else:
                                agg_group[alias] = {
                                    "$sum": {"$cond": [f"${field_name}", 1, 0]}
                                }
                        elif func_name == "min":
                            agg_group[alias] = {"$min": f"${field_name}"}
                        elif func_name == "max":
                            agg_group[alias] = {"$max": f"${field_name}"}
                        else:
                            raise NotImplementedError(
                                f"Function {func_name} not supported"
                            )
                elif isinstance(col, Constant):
                    val = str(
                        col.value
                    )  # Convert to string becuase it is interpreted as an index.
                    if col.alias is None:
                        alias = val
                    else:
                        alias = col.alias.parts[-1]
                    project[alias] = val

        if node.group_by is not None:
            # TODO
            raise NotImplementedError(f"Group {node.group_by}")

        sort = {}
        if node.order_by is not None:
            for col in node.order_by:
                name = col.field.parts[-1]
                direction = 1 if col.direction.upper() == "ASC" else -1
                sort[name] = direction

        # Compose the MongoDB query.
        mquery = MongoQuery(collection)

        method = "aggregate"
        arg = []

        # MongoDB related pipeline steps for the aggregate method.
        if node.modifiers is not None:
            for modifier in node.modifiers:
                arg.append(modifier)

        if pre_match:
            arg.append({"$match": pre_match})
        if pre_project is not None and pre_project != {}:
            arg.append({"$project": pre_project})

        if filters:
            arg.append({"$match": filters})

        if group:
            arg.append({"$group": group})

        if project:
            arg.append({"$project": project})

        if sort:
            arg.append({"$sort": sort})

        if node.offset is not None:
            arg.append({"$skip": int(node.offset.value)})

        if node.limit is not None:
            arg.append({"$limit": int(node.limit.value)})

        mquery.add_step({"method": method, "args": [arg]})

        print("&***********************************************************")
        print(f"DEBUG: MongoDB query pipeline: {arg}")
        print(method)
        print("***********************************************************&")

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
        if type(node) not in [BinaryOperation]:
            raise NotImplementedError(f"Not supported type {type(node)}")

        op = node.op.lower()
        arg1, arg2 = node.args

        if op in ("and", "or"):
            query1 = self.handle_where(arg1)
            query2 = self.handle_where(arg2)

            ops = {
                "and": "$and",
                "or": "$or",
            }
            query = {ops[op]: [query1, query2]}
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

        if isinstance(arg1, Identifier):
            var_name = ".".join(arg1.parts)
            # Simple operation.
            if isinstance(arg2, Constant):
                # Identifier and Constant.
                val = ObjectId(arg2.value) if var_name == "_id" else arg2.value
                if op in ("=", "=="):
                    pass
                elif op in ops_map:
                    op2 = ops_map[op]
                    val = {op2: val}
                else:
                    raise NotImplementedError(f"Not supported operator {op}")

                return {var_name: val}

            # IN condition.
            elif isinstance(arg2, Tuple):
                # Should be IN, NOT IN.
                ops = {"in": "$in", "not in": "$nin"}
                # Must be list of Constants.
                values = [i.value for i in arg2.items]

                if op in ops:
                    op2 = ops[op]
                    cond = {op2: values}
                else:
                    raise NotImplementedError(f"Not supported operator {op}")

                return {var_name: cond}

        # Create expression.
        val1 = self.where_element_convert(arg1)
        val2 = self.where_element_convert(arg2)

        if op in ops_map:
            op2 = ops_map[op]
        else:
            raise NotImplementedError(f"Not supported operator {op}")

        return {"$expr": {op2: [val1, val2]}}

    def where_element_convert(
        self, node: Union[Identifier, Latest, Constant, TypeCast]
    ) -> Any:
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
