from enum import Enum


class Nodes:
    def __init__(self, enum: Enum) -> None:
        self.enum = enum


class Extract:
    def __init__(self, obj, key: str):
        self.obj = obj
        self.key = key


class DeepExtract:
    def __init__(self, path: list[str], mysql_data_type: str, description: str = None):
        if len(path) < 2:
            raise ValueError(f"Minimum length of path for DeepExtract is 2: {path}")
        self.path = path
        self.mysql_data_type = mysql_data_type
        if description is None:
            path_str = path[0] + ''.join(f'["{p}"]' for p in path[1:])
            self.description = f'Value is extracted from {path_str}'
        else:
            self.description = description
