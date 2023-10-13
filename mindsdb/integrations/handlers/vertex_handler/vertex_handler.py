from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.vertex_handler.vertex_client import VertexClient, PATH_TO_SERVICE_ACCOUNT_JSON, PROJECT_ID
import pandas as pd


class VertexHandler(BaseMLEngine):
    """Handler for the Vertex Google AI cloud API"""

    name = "Vertex"

    def create(self, target, df, args={}):
        """Logs in to Vertex and deploy a pre-trained model to an endpoint.
        
        If the endpoint already exists for the model, we do nothing.

        If the endpoint does not exist, we create it and deploy the model to it.
        The runtime for this is long, it took 15 minutes for a small model. 
        """
        model_name = args["using"]["model_name"]
        vertex = VertexClient(PATH_TO_SERVICE_ACCOUNT_JSON, PROJECT_ID)
        model = vertex.get_model_by_display_name(model_name)
        if not model:
            print("Model not found")
            return
        endpoint_name = model_name + "_endpoint"
        if vertex.get_endpoint_by_display_name(endpoint_name):
            print("Endpoint already exists")
        else:
            endpoint = vertex.deploy_model(model)
            endpoint.display_name = endpoint_name
            endpoint.update()
            print("Endpoint deployed")
        predict_args = {}
        predict_args["endpoint_name"] = endpoint_name
        self.model_storage.json_set("predict_args", predict_args)
        

    def predict(self, df, args={}):
        """Predict using the deployed model."""
        predict_args = self.model_storage.json_get("predict_args")
        vertex = VertexClient(PATH_TO_SERVICE_ACCOUNT_JSON, PROJECT_ID)
        results = vertex.predict_from_df(predict_args["endpoint_name"], df)
        return pd.DataFrame(results.predictions)
