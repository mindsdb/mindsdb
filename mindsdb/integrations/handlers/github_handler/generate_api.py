import inspect
from typing import List
from dataclasses import dataclass

import pandas as pd
import github

from mindsdb.integrations.utilities.sql_utils import (FilterCondition, FilterOperator, SortColumn)
from mindsdb.integrations.libs.api_handler import APIResource


@dataclass
class Type:
    name: str
    sub_type: str = None
    optional: bool = False


@dataclass
class GHMethod:
    name: str
    table_name: str
    params: dict
    output: Type


def parse_annotations(annotations):
    '''
    Parse string annotation, and extract type, input examples:
    - Milestone | Opt[str]
    - PaginatedList[Issue]
    '''
    type_name, sub_type = None, None
    if not isinstance(annotations, str):

        return Type(getattr(annotations, '__name__', None))
    for item in annotations.split('|'):
        item = item.strip()
        if item is None:
            continue
        if '[' in item:
            type_name = item[: item.find('[')]
            item2 = item[item.find('[') + 1: item.rfind(']')]
            if type_name == 'Opt':
                inner_type = parse_annotations(item2)
                inner_type.optional = Type
                return inner_type
            if type_name == 'dict':
                item2 = item2[item2.find(',') + 1:]
            sub_type = parse_annotations(item2).name
        else:
            type_name = item
        # get only first type
        break
    return Type(type_name, sub_type)


def get_properties(cls):
    # find properties of the class

    properties = {}
    for prop_name, prop in inspect.getmembers(cls):
        if prop_name.startswith('_'):
            continue
        if not isinstance(prop, property):
            continue
        sig2 = inspect.signature(prop.fget)

        properties[prop_name] = parse_annotations(sig2.return_annotation)
    return properties


def get_github_types():
    # get github types
    types = {}

    GithubObject = github.GithubObject.GithubObject
    for module_name, module in inspect.getmembers(github, inspect.ismodule):
        cls = getattr(module, module_name, None)
        if cls is None:
            continue
        if issubclass(cls, GithubObject):

            # remove inherited props
            parent_props = []
            for cls2 in cls.__bases__:
                parent_props += get_properties(cls2).keys()

            properties = {}
            for k, v in get_properties(cls).items():
                if k not in parent_props:
                    properties[k] = v

            types[module_name] = properties
    return types


def get_github_methods(cls):
    '''
    Analyse class in order to find methods which return list of objects.
    '''
    methods = []

    for method_name, method in inspect.getmembers(cls, inspect.isfunction):
        sig = inspect.signature(method)

        return_type = parse_annotations(sig.return_annotation)
        list_prefix = 'get_'
        if not (method_name.startswith(list_prefix) and return_type.name == 'PaginatedList'):
            continue

        table_name = method_name[len(list_prefix):]

        params = {}
        for param_name, param in sig.parameters.items():
            params[param_name] = parse_annotations(param.annotation)

        methods.append(GHMethod(
            name=method_name,
            table_name=table_name,
            params=params,
            output=return_type
        ))
    return methods


class GHTable(APIResource):
    def __init__(self, *args, method: GHMethod = None, github_types=None, **kwargs):
        self.method = method
        self.github_types = github_types

        self.output_columns = {}
        if method.output.sub_type in self.github_types:
            self.output_columns = self.github_types[method.output.sub_type]

        # check params:
        self.params, self.list_params = [], []
        for name, param_type in method.params.items():
            self.params.append(name)
            if param_type.name == 'list':
                self.list_params.append(name)

        self._allow_sort = 'sort' in method.params

        super().__init__(*args, **kwargs)

    def repr_value(self, value, type_name):
        if value is None or type_name in ('bool', 'int', 'float'):
            return value
        if type_name in self.github_types:
            properties = self.github_types[type_name]
            if 'login' in properties:
                value = getattr(value, 'login')
            elif 'url' in properties:
                value = getattr(value, 'url')
        return str(value)

    def get_columns(self) -> List[str]:
        return list(self.output_columns.keys())

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ) -> pd.DataFrame:

        if limit is None:
            limit = 20

        method_kwargs = {}
        if sort is not None and self._allow_sort:
            for col in sort:
                method_kwargs['sort'] = col.column
                method_kwargs['direction'] = 'asc' if col.ascending else 'desc'
                sort.applied = True
                # supported only 1 column
                break

        if conditions:
            for condition in conditions:
                if condition.column not in self.params:
                    continue

                if condition.column in self.list_params:
                    if condition.op == FilterOperator.IN:
                        method_kwargs[condition.column] = condition.value
                    elif condition.op == FilterOperator.EQUAL:
                        method_kwargs[condition.column] = [condition]
                    condition.applied = True
                else:
                    method_kwargs[condition.column] = condition.value
                    condition.applied = True

        connection = self.handler.connect()
        method = getattr(connection.get_repo(self.handler.repository), self.method.name)

        data = []
        count = 0
        for record in method(**method_kwargs):
            item = {}
            for name, output_type in self.output_columns.items():

                # workaround to prevent making addition request per property.
                if name in targets:
                    # request only if is required
                    value = getattr(record, name)
                else:
                    value = getattr(record, '_' + name).value
                if value is not None:
                    if output_type.name == 'list':
                        value = ",".join([
                                str(self.repr_value(i, output_type.sub_type))
                                for i in value
                        ])
                    else:
                        value = self.repr_value(value, output_type.name)
                item[name] = value

            data.append(item)

            count += 1
            if limit <= count:
                break

        return pd.DataFrame(data, columns=self.get_columns())
