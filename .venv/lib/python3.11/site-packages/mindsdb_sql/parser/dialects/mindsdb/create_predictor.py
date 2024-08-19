import json
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent
from mindsdb_sql.parser.ast.select import Identifier
from mindsdb_sql.parser.ast.select.operation import Object


class CreatePredictorBase(ASTNode):
    def __init__(self,
                 name,
                 targets=None,
                 integration_name=None,
                 query_str=None,
                 order_by=None,
                 group_by=None,
                 window=None,
                 horizon=None,
                 using=None,
                 is_replace=False,
                 if_not_exists=False,
                 task=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.integration_name = integration_name
        self.query_str = query_str
        self.targets = targets
        self.order_by = order_by
        self.group_by = group_by
        self.window = window
        self.horizon = horizon
        self.using = using
        self.is_replace = is_replace
        self.if_not_exists = if_not_exists
        self.task = task
        self._action = 'CREATE'

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        name_str = f'\n{ind1}name={self.name.to_tree()},'

        if self.integration_name is not None:
            integration_name_str = f'\n{ind1}integration_name={self.integration_name.to_tree()},'
        else:
            integration_name_str = 'None'

        query_str = f'\n{ind1}query={self.query_str},'

        if self.targets is not None:
            target_trees = ',\n'.join([t.to_tree(level=level+2) for t in self.targets])
            targets_str = f'\n{ind1}targets=[\n{target_trees}\n{ind1}],'
        else:
            targets_str = ''

        group_by_str = ''
        if self.group_by:
            group_by_trees = ',\n'.join([t.to_tree(level=level+2) for t in self.group_by])
            group_by_str = f'\n{ind1}group_by=[\n{group_by_trees}\n{ind1}],'

        order_by_str = ''
        if self.order_by:
            order_by_trees = ',\n'.join([t.to_tree(level=level + 2) for t in self.order_by])
            order_by_str = f'\n{ind1}order_by=[\n{order_by_trees}\n{ind1}],'

        window_str = f'\n{ind1}window={repr(self.window)},'
        horizon_str = f'\n{ind1}horizon={repr(self.horizon)},'
        using_str = f'\n{ind1}using={repr(self.using)},'

        if_not_exists_str = f'\n{ind1}if_not_exists={self.if_not_exists},' if self.if_not_exists else ''
        or_replace_str = f'\n{ind1}is_replace={self.is_replace},' if self.is_replace else ''

        out_str = f'{ind}{self.__class__.__name__}(' \
                  f'{or_replace_str}' \
                  f'{if_not_exists_str}' \
                  f'{name_str}' \
                  f'{integration_name_str}' \
                  f'{query_str}' \
                  f'{targets_str}' \
                  f'{order_by_str}' \
                  f'{group_by_str}' \
                  f'{window_str}' \
                  f'{horizon_str}' \
                  f'{using_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        if self.targets is not None:
            targets_str = 'PREDICT ' + ', '.join([out.to_string() for out in self.targets])
        else:
            targets_str = ''
        order_by_str = f'ORDER BY {", ".join([out.to_string() for out in self.order_by])} ' if self.order_by else ''
        group_by_str = f'GROUP BY {", ".join([out.to_string() for out in self.group_by])} ' if self.group_by else ''
        window_str = f'WINDOW {self.window} ' if self.window is not None else ''
        horizon_str = f'HORIZON {self.horizon} ' if self.horizon is not None else ''
        using_str = ''
        if self.using:
            using_ar = []
            for key, value in self.using.items():
                if isinstance(value, Object):
                    args = [
                        f'{k}={json.dumps(v)}'
                        for k, v in value.params.items()
                    ]
                    args_str = ', '.join(args)
                    value = f'{value.type}({args_str})'
                else:
                    value = json.dumps(value)

                using_ar.append(f'{Identifier(key).to_string()}={value}')

            using_str = f'USING ' + ', '.join(using_ar)

        query_str = ''
        if self.query_str is not None:
            integration_name_str = ''
            if self.integration_name is not None:
                integration_name_str = f' {self.integration_name.to_string()}'

            query_str = f'FROM{integration_name_str} ({self.query_str}) '

        or_replace_str = ' OR REPLACE' if self.is_replace else ''
        if_not_exists_str = 'IF NOT EXISTS ' if self.if_not_exists else ''
        object_str = self._object + ' ' if self._object else ''

        out_str = f'{self._action}{or_replace_str} {object_str}{if_not_exists_str}{self.name.to_string()} {query_str}' \
                  f'{targets_str} ' \
                  f'{order_by_str}' \
                  f'{group_by_str}' \
                  f'{window_str}' \
                  f'{horizon_str}' \
                  f'{using_str}'

        return out_str.strip()


class CreatePredictor(CreatePredictorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._object = 'MODEL'


# Models by task type
class CreateAnomalyDetectionModel(CreatePredictorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._object = 'ANOMALY DETECTION MODEL'
        self.task = Identifier('AnomalyDetection')
