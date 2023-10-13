from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.vertex_handler.vertex_client import VertexClient


class VertexHandler(BaseMLEngine):
    """Handler for the Vertex Google AI cloud API"""

    name = "Vertex"

    def create(self, target, df, args={}):
        """Deploy a pre-trained model in Vertex to an endpoint.
        
        If the endpoint already exists for the model, we do nothing.

        If the endpoint does not exist, we create it and deploy the model to it.
        The runtime for this is long, it took 15 minutes for a small model. 
        """
        model_name = args["model_name"]
        vertex = VertexClient(PATH_TO_SERVICE_ACCOUNT_JSON, PROJECT_ID)
        endpoint_name = model_name + "_endpoint"
        if vertex.get_endpoint_by_display_name(endpoint_name):
            print("Endpoint already exists")
            return

        model = vertex.get_model_by_display_name(model_name)
        if not model:
            print("Model not found")
            return
        endpoint = vertex.deploy_model(model)
        endpoint.display_name = endpoint_name
        endpoint.update()
        print("Endpoint deployed")
        

    def predict(self, df, args={}):
        pass