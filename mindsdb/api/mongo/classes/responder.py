class Responder():
    def __init__(self, when=None, result=None):
        if when:
            self.when = when
        if result:
            self.result = result
        if not hasattr(self, 'when') or (not isinstance(self.when, dict) and not callable(self.when)):
            raise ValueError("Responder attr 'when' must be dict or function.")
        if not hasattr(self, 'result') or (not isinstance(self.result, dict) and not callable(self.result)):
            raise ValueError("Responder attr 'result' must be dict or function.")

    def match(self, query):
        """ check, if this 'responder' can be used to answer or current request

        query (dict): request document

        return bool
        """
        if isinstance(self.when, dict):
            for key, value in self.when.items():
                if key not in query:
                    return False
                if callable(value):
                    if not value(query[key]):
                        return False
                elif value != query[key]:
                    return False
            return True
        else:
            return self.when(query)

    def handle(self, query, args, env):
        """ making answer based on params:

        query (dict): document(s) from request
        args (dict): all other significant information from request: flags, collection name, rows to return, etc
        env (dict): config, mindsdb_native instance, and other mindsdb related stuff

        returns documents as dict or list of dicts
        """
        if isinstance(self.result, dict):
            return self.result
        else:
            return self.result(query, args, env)
