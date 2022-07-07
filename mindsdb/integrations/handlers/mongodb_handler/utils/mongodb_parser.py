
import json
import ast as py_ast

import dateutil.parser
from bson import ObjectId

from .mongodb_query import MongoQuery


class MongodbParser:
    '''
        converts string into MongoQuery
    '''

    def from_string(self, call_str):
        tree = py_ast.parse(call_str, mode='eval')
        calls = self.process(tree.body)
        # first call contents collection
        method1 = calls[0]['method']
        if len(method1) < 2:
            raise IndexError('Collection not found')
        collection = method1[-2]

        mquery = MongoQuery(collection)

        # keep only last name
        calls[0]['method'] = [method1[-1]]

        # convert method names get first item of list
        for c in calls:
            mquery.add_step({
                'method': c['method'][0],
                'args': c['args']
            })

        return mquery

        # return {
        #     'collection': collection,
        #     'call': calls
        # }

    def process(self, node):
        if isinstance(node, py_ast.Call):
            previous_call = None

            args = []
            for node2 in node.args:
                args.append(self.process(node2))

            # check functions
            if isinstance(node.func, py_ast.Name):
                # it is just name
                func = node.func.id

                # special functions:
                if func == 'ISODate':
                    return dateutil.parser.isoparse(args[0])
                if func == 'ObjectId':
                    return ObjectId(args[0])
            elif isinstance(node.func, py_ast.Attribute):
                # it can be an attribute or pipeline
                previous_call, func = self.process_func_name(node.func)
            else:
                raise NotImplementedError(f'Unknown function type: {node.func}')

            call = [{
                'method': func,
                'args': args
            }]
            if previous_call is not None:
                call = previous_call + call

            return call

        if isinstance(node, py_ast.List):
            elements = []
            for node2 in node.elts:
                elements.append(self.process(node2))
            return elements

        if isinstance(node, py_ast.Dict):

            keys = []
            for node2 in node.keys:
                if not isinstance(node2, py_ast.Constant):
                    raise NotImplementedError(f'Unknown dict key {node2}')
                keys.append(node2.value)

            values = []
            for node2 in node.values:
                values.append(self.process(node2))

            return dict(zip(keys, values))

        if isinstance(node, py_ast.Name):
            # special attributes
            name = node.id
            if name == 'true':
                return True
            elif name == 'false':
                return False
            elif name == 'null':
                return None

        if isinstance(node, py_ast.Constant):
            return node.value

        if isinstance(node, py_ast.UnaryOp):
            if isinstance(node.op, py_ast.USub):
                value = self.process(node.operand)
                return -value

        raise NotImplementedError(f'Unknown node {node}')

    def process_func_name(self, node):
        previous_call = None
        if isinstance(node, py_ast.Attribute):
            attribute = node
            # multilevel attribute

            obj_name = []
            while isinstance(attribute, py_ast.Attribute):
                obj_name.insert(0, attribute.attr)
                attribute = attribute.value

            if isinstance(attribute, py_ast.Name):
                obj_name.insert(0, attribute.id)

            if isinstance(attribute, py_ast.Call):
                # is pipeline
                previous_call = self.process(attribute)

            return previous_call, obj_name
