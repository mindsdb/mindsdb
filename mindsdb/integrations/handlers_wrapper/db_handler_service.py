import os
import json
from mindsdb.utilities import log
from mindsdb.integrations.handlers_wrapper.db_handler_wrapper import DBHandlerWrapper


if __name__ == "__main__":
    params = os.environ.get('PARAMS', '{}')
    kwargs = json.loads(params)
    if "name" not in kwargs:
        kwargs["name"] = __name__
    app = DBHandlerWrapper(**kwargs)
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '0.0.0.0')
    log.logger.info("Running dbservice: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)
