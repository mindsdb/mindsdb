from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.utils import indent


class Operation(ASTNode):
    def __init__(self, op, args, *args_, **kwargs):
        super().__init__(*args_, **kwargs)

        self.op = ' '.join(op.lower().split())
        self.args = list(args)
        self.assert_arguments()

    def assert_arguments(self):
        pass

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        arg_trees = [arg.to_tree(level=level+2) for arg in self.args]
        arg_trees_str = ",\n".join(arg_trees)
        out_str = f'{ind}{self.__class__.__name__}(op={repr(self.op)},\n{ind1}args=(\n{arg_trees_str}\n{ind1})\n{ind})'
        return out_str

    def get_string(self, *args, alias=True, **kwargs):
        arg_strs = [arg.to_string() for arg in self.args]
        args_str = ','.join(arg_strs)

        return f'{self.op}({args_str})'


class BetweenOperation(Operation):
    def __init__(self, *args, **kwargs):
        super().__init__(op='between', *args, **kwargs)

    def get_string(self, *args, **kwargs):
        arg_strs = [arg.to_string() for arg in self.args]
        return f'{arg_strs[0]} BETWEEN {arg_strs[1]} AND {arg_strs[2]}'


class BinaryOperation(Operation):
    def get_string(self, *args, **kwargs):
        arg_strs = []
        for arg in self.args:
            arg_str = arg.to_string()
            # if isinstance(arg, BinaryOperation) or isinstance(arg, BetweenOperation):
            #     # to parens
            #     arg_str = f'({arg_str})'
            arg_strs.append(arg_str)

        return f'{arg_strs[0]} {self.op.upper()} {arg_strs[1]}'

    def assert_arguments(self):
        if len(self.args) != 2:
            raise ParsingException(f'Expected two arguments for operation "{self.op}"')


class UnaryOperation(Operation):
    def get_string(self, *args, **kwargs):
        return f'{self.op} {self.args[0].to_string()}'

    def assert_arguments(self):
        if len(self.args) != 1:
            raise ParsingException(f'Expected one argument for operation "{self.op}"')


class Function(Operation):
    def __init__(self, *args, distinct=False, from_arg=None, namespace=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.distinct = distinct
        self.from_arg = from_arg
        self.namespace = namespace

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        arg_trees = [arg.to_tree(level=level+2) for arg in self.args]
        arg_trees_str = ",\n".join(arg_trees)
        alias_str = f'alias={self.alias.to_tree()},' if self.alias else ''
        from_str = f'from={self.from_arg.to_tree()}' if self.from_arg else ''
        out_str = f'{ind}{self.__class__.__name__}(op={repr(self.op)}, distinct={repr(self.distinct)},{alias_str}\n' \
                  f'{ind1}args=[\n' \
                  f'{arg_trees_str}\n' \
                  f'{ind1}]\n' \
                  f'{ind1}{from_str}\n' \
                  f'{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        args_str = ', '.join([arg.to_string() for arg in self.args])
        distinct_str = 'DISTINCT ' if self.distinct else ''

        from_str = f' FROM {self.from_arg.to_string()}' if self.from_arg else ''
        namespace = self.namespace + '.' if self.namespace else ''
        return f'{namespace}{self.op}({distinct_str}{args_str}{from_str})'


class WindowFunction(ASTNode):
    def __init__(self, function, partition=None, order_by=None, alias=None):
        super().__init__()
        self.function = function
        self.partition = partition
        self.order_by = order_by
        self.alias = alias

    def to_tree(self, *args, level=0, **kwargs):
        fnc_str = self.function.to_tree(level=level+2)
        ind = indent(level)
        ind1 = indent(level+1)
        partition_str = ''
        if self.partition is not None:
            partition_str = f',\n'.join([arg.to_tree(level=level+2) for arg in self.partition])
            partition_str = f'\n{ind1}partition=\n{partition_str}'

        order_str = ''
        if self.order_by is not None:
            order_str = f'\n{ind1}order_by=\n' + ',\n'.join([arg.to_tree(level=level+2) for arg in self.order_by])

        if self.alias is not None:
            alias_str = f'\n{ind1}alias=' + self.alias.to_string()
        else:
            alias_str = ''
        return f'{ind}WindowFunction(\n' \
               f'{ind1}function=\n{fnc_str}' \
               f'{partition_str}' \
               f'{order_str}' \
               f'{alias_str}' \
               f'\n{ind})'

    def to_string(self, *args, **kwargs):
        fnc_str = self.function.get_string()
        partition_str = ''
        if self.partition is not None:
            partition_str = 'PARTITION BY ' + ', '.join([arg.to_string() for arg in self.partition])

        order_str = ''
        if self.order_by is not None:
            order_str = 'ORDER BY ' + ', '.join([arg.to_string() for arg in self.order_by])

        if self.alias is not None:
            alias_str = self.alias.to_string()
        else:
            alias_str = ''
        return f'{fnc_str} over({partition_str} {order_str}) {alias_str}'


class Object(ASTNode):
    def __init__(self, type, params=None, **kwargs):
        super().__init__(**kwargs)

        self.type = type
        self.params = params

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)

        params = [
            f'{k}={v}'
            for k, v in self.params.items()
        ]
        params_str = ': '.join(params)

        return f'{ind}Object(type={repr(self.type)}, params={{params_str}})'

    def to_string(self, *args, **kwargs):
        return self.to_tree()

    def __repr__(self):
        return self.to_tree()


class Interval(Operation):

    def __init__(self, info):
        super().__init__(op='interval', args=[info, ])

    def get_string(self, *args, **kwargs):
        return f'INTERVAL {repr(self.args[0])}'

    def to_tree(self, *args, level=0, **kwargs):
        return self.get_string( *args, **kwargs)

    def assert_arguments(self):
        if len(self.args) != 1:
            raise ParsingException(f'Expected one argument for operation "{self.op}"')
