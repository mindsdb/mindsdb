from mindsdb.api.mongo.classes import Responder
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx


logger = log.getLogger(__name__)


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        user_id = query.get('user-id')
        company_id = query.get('company_id')
        user_class = query.get('user_class', 0)
        encryption_key = query.get('encryption_key')
        email_confirmed = query.get('email_confirmed', 1)

        need_response = query.get('need_response', False)

        ctx.user_id = user_id
        ctx.company_id = company_id
        ctx.user_class = user_class
        ctx.encryption_key = encryption_key
        ctx.email_confirmed = email_confirmed

        if Config().get("cloud", False) is True and ctx.encryption_key is None:
            logger.warn("Got cloud Mongo request with missed encryption key")
            # TODO Raise exception here after cloud migrations

        if need_response:
            return {'ok': 1}

        return None


responder = Responce()
