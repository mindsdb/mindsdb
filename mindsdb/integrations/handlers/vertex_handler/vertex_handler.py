import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.vertex_handler.vertex_client import VertexClient
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class VertexHandler(BaseMLEngine):
    """Handler for the Vertex Google AI cloud API"""

    name = "Vertex"

    def create(self, target, args=None, **kwargs):
        """Logs in to Vertex and deploy a pre-trained model to an endpoint.

        If the endpoint already exists for the model, we do nothing.

        If the endpoint does not exist, we create it and deploy the model to it.
        The runtime for this is long, it took 15 minutes for a small model.
        """
        assert "using" in args, "Must provide USING arguments for this handler"
        args = args["using"]

        model_name = args.pop("model_name")
        custom_model = args.pop("custom_model", False)

        # get credentials from engine
        credentials_url, credentials_file, credentials_json = self._get_credentials_from_engine()

        # get vertex args from handler then update args from model
        vertex_args = self.engine_storage.json_get('args')
        vertex_args.update(args)

        vertex = VertexClient(vertex_args, credentials_url, credentials_file, credentials_json)

        model = vertex.get_model_by_display_name(model_name)
        if not model:
            raise Exception(f"Vertex model {model_name} not found")
        endpoint_name = model_name + "_endpoint"
        if vertex.get_endpoint_by_display_name(endpoint_name):
            logger.info(f"Endpoint {endpoint_name} already exists, skipping deployment")
        else:
            logger.info(f"Starting deployment at {endpoint_name}")
            endpoint = vertex.deploy_model(model)
            endpoint.display_name = endpoint_name
            endpoint.update()
            logger.info(f"Endpoint {endpoint_name} deployed")

        predict_args = {}
        predict_args["target"] = target
        predict_args["endpoint_name"] = endpoint_name
        predict_args["custom_model"] = custom_model
        self.model_storage.json_set("predict_args", predict_args)
        self.model_storage.json_set("vertex_args", vertex_args)

    def predict(self, df, args=None):
        """Predict using the deployed model by calling the endpoint."""

        if "__mindsdb_row_id" in df.columns:
            df.drop("__mindsdb_row_id", axis=1, inplace=True)  # TODO is this required?

        predict_args = self.model_storage.json_get("predict_args")
        vertex_args = self.model_storage.json_get("vertex_args")

        # get credentials from engine
        credentials_url, credentials_file, credentials_json = self._get_credentials_from_engine()

        vertex = VertexClient(vertex_args, credentials_url, credentials_file, credentials_json)
        results = vertex.predict_from_df(predict_args["endpoint_name"], df, custom_model=predict_args["custom_model"])

        if predict_args["custom_model"]:
            return pd.DataFrame(results.predictions, columns=[predict_args["target"]])
        else:
            return pd.DataFrame(results.predictions)

    def create_engine(self, connection_args):
        # check if one of credentials_url, credentials_file, or credentials_json is provided
        if 'service_account_key_url' not in connection_args and 'service_account_key_file' not in connection_args and 'service_account_key_json' not in connection_args:
            raise KeyError('Either service_account_key_url, service_account_key_file, or service_account_key_json must be provided')

        self.engine_storage.json_set('args', connection_args)

    def _get_credentials_from_engine(self):
        engine_args = self.engine_storage.json_get('args')

        return engine_args.get('service_account_key_url'), engine_args.get('service_account_key_file'), engine_args.get('service_account_key_json')
