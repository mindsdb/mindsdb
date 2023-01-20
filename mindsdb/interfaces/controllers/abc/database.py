from abc import ABC, abstractmethod


class Database(ABC):
    name: str

    @staticmethod
    @abstractmethod
    def from_record(record):
        pass
