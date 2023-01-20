from abc import ABC, abstractmethod


class Collection(ABC):
    @abstractmethod
    def all(self):
        pass

    # @abstractmethod
    # def get(self, name, type):
    #     pass

    @abstractmethod
    def __contains__(self, key):
        pass

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

    @abstractmethod
    def __delitem__(self, key):
        pass

    def __iter__(self):
        items = self.all()
        return iter(items)

    def __len__(self):
        items = self.all()
        return len(items)
