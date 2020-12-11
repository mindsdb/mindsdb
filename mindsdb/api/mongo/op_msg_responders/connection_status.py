from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'connectionStatus': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        res = {
            'authInfo': {
                'authenticatedUsers': [],
                'authenticatedUserRoles': []
            },
            'ok': 1
        }
        if helpers.is_true(query.get('showPrivileges')):
            res['authInfo']['authenticatedUserPrivileges'] = []

        return res


responder = Responce()
