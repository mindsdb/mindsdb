import os
import json
from mindsdb.utilities.log import get_log
from mindsdb.integrations.handlers_wrapper.ml_handler_wrapper import MLHandlerWrapper


log = get_log()


if __name__ == "__main__":
    params = os.environ.get('PARAMS', '{}')
    kwargs = json.loads(params)
    if "name" not in kwargs:
        kwargs["name"] = __name__
    app = MLHandlerWrapper(**kwargs)
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '0.0.0.0')
    log.info("Running ML service: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)
