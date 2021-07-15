from mindsdb.utilities.config import Config

from mindsdb.api.http.initialize import initialize_flask

app, _ = initialize_flask(Config(), None, None)
