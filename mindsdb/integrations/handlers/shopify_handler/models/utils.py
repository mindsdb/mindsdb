from enum import Enum


class Nodes:
    def __init__(self, enum: Enum) -> None:
        self.enum = enum


class Extract:
    def __init__(self, obj, key):
        self.obj = obj
        self.key = key
