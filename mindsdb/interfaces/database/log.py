from typing import List

from mindsdb.utilities.exception import EntityNotExistsError


class MindsdbDBTable:
    pass


class SystemTable(MindsdbDBTable):
    pass


class LogTable(SystemTable):
    def __init__(self, name: str) -> None:
        self.name = name


class LogDBController:
    def __init__(self):
        self._tables = {
            'llm_log': LogTable('llm_log')
        }

    def get_list(self) -> List[LogTable]:
        return list(self._tables.values())

    def get(self, name: str = None) -> LogTable:
        try:
            return self._tables[name]
        except KeyError:
            raise EntityNotExistsError(f'Table log.{name} does not exists')
