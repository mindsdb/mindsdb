from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Update(ASTNode):
    def __init__(self,
                 table,
                 update_columns=None,
                 keys=None,
                 from_select=None,
                 from_select_alias=None,
                 where=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table = table
        # list[Identifier]
        self.keys = keys
        # dict: {str: Identifier}
        self.update_columns = update_columns
        self.where = where
        self.from_select = from_select
        self.from_select_alias = from_select_alias

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)

        updated_str = ''
        if self.update_columns is not None:
            updated_ar = [
                f'{k}={v.to_string()}'
                for k, v in self.update_columns.items()
            ]
            updated_str = ', '.join(updated_ar)
            updated_str = f'{ind1}update_columns={updated_str}\n'

        keys_str = ''
        if self.keys is not None:
            keys_ar = [k.to_string() for k in self.keys]
            keys_str = ', '.join(keys_ar)
            keys_str = f'{ind1}keys={keys_str}\n'

        where_str = ''
        if self.where is not None:
            where_str = ind1 + self.where.to_tree()

        if self.from_select is not None:
            from_select_str = f'{ind1}from_select=\n{self.from_select.to_tree(level=level+2)}\n'
            if self.from_select_alias is not None:
                from_select_str += f'{ind1}from_select_alias=\n{self.from_select_alias.to_tree(level=level+2)}\n'

        else:
            from_select_str = ''

        out_str = f'{ind}Update(table={self.table.to_tree()}\n' \
                  f'{keys_str}' \
                  f'{updated_str}' \
                  f'{where_str}' \
                  f'{from_select_str}' \
                  f'{ind})\n'
        return out_str

    def get_string(self, *args, **kwargs):
        update_str = ''
        if self.update_columns is not None:
            update_ar = [
                f'{k}={v.to_string()}'
                for k, v in self.update_columns.items()
            ]
            update_str = ' set ' + ', '.join(update_ar)

        keys_str = ''
        if self.keys is not None:
            keys_ar = [k.to_string() for k in self.keys]
            keys_str = ' on ' + ', '.join(keys_ar)

        if self.from_select is not None:
            alias_str = ''
            if self.from_select_alias is not None:
                alias_str = ' as ' + self.from_select_alias.to_string()
            from_select_str = f' from ({self.from_select.to_string()}){alias_str}'
        else:
            from_select_str = ''

        where_str = ''
        if self.where is not None:
            where_str = ' where ' + self.where.to_string()

        return f'update {self.table.to_string()}{keys_str}{update_str}{from_select_str}{where_str}'
