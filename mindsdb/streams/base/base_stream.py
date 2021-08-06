

class BaseStream:
    def read(self):
        raise NotImplementedError

    def write(self, dct):
        raise NotImplementedError
