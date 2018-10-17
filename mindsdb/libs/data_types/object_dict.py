import logging

class ObjectDict():

    def getAsDict(self):
        if hasattr(self, '_ignore_keys'):
            ignore_keys = self._ignore_keys
        else:
            ignore_keys = []
        ret = {key:self.__dict__[key] for key in self.__dict__ if key[0] != '_' and key not in ignore_keys}
        return ret

    def setFromDict(self, dict):
        """
        This tries to populate object from a dictionary
        :param dict: dict
        :return: None
        """
        for key in dict:
            if key in self.__dict__:
                self.__setattr__(key, dict[key])
            else:
                logging.warn('no {key} in class'.format(key=key))

