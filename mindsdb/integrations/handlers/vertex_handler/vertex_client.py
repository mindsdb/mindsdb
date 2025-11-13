from mindsdb.utilities import log
from google.cloud.aiplatform import init, TabularDataset, Model, Endpoint
import pandas as pd

from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleServiceAccountOAuth2Manager

logger = log.getLogger(__name__)


class VertexClient:
    """A class to interact with Vertex AI"""

    def __init__(self, args_json, credentials_url=None, credentials_file=None, credentials_json=None):
        google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
            credentials_url=credentials_url,
            credentials_file=credentials_file,
            credentials_json=credentials_json,
        )
        credentials = google_sa_oauth2_manager.get_oauth2_credentials()

        init(
            credentials=credentials,
            project=args_json["project_id"],
            location=args_json["location"],
            staging_bucket=args_json["staging_bucket"],
            # the name of the experiment to use to track
            # logged metrics and parameters
            experiment=args_json["experiment"],
            # description of the experiment above
            experiment_description=args_json["experiment_description"],
        )

    def print_datasets(self):
        """Print all datasets and dataset ids in the project"""
        for dataset in TabularDataset.list():
            logger.info(f"Dataset display name: {dataset.display_name}, ID: {dataset.name}")

    def print_models(self):
        """Print all model names and model ids in the project"""
        for model in Model.list():
            logger.info(f"Model display name: {model.display_name}, ID: {model.name}")

    def print_endpoints(self):
        """Print all endpoints and endpoint ids in the project"""
        for endpoint in Endpoint.list():
            logger.info(f"Endpoint display name: {endpoint.display_name}, ID: {endpoint.name}")

    def get_model_by_display_name(self, display_name):
        """Get a model by its display name"""
        try:
            return Model.list(filter=f"display_name={display_name}")[0]
        except IndexError:
            logger.info(f"Model with display name {display_name} not found")

    def get_endpoint_by_display_name(self, display_name):
        """Get an endpoint by its display name"""
        try:
            return Endpoint.list(filter=f"display_name={display_name}")[0]
        except IndexError:
            logger.info(f"Endpoint with display name {display_name} not found")

    def get_model_by_id(self, model_id):
        """Get a model by its ID"""
        try:
            return Model(model_name=model_id)
        except IndexError:
            logger.info(f"Model with ID {model_id} not found")

    def deploy_model(self, model):
        """Deploy a model to an endpoint - long runtime"""
        endpoint = model.deploy()
        return endpoint

    def predict_from_df(self, endpoint_display_name, df, custom_model=False):
        """Make a prediction from a Pandas dataframe"""
        endpoint = self.get_endpoint_by_display_name(endpoint_display_name)
        if custom_model:
            records = df.values.tolist()
        else:
            records = df.astype(str).to_dict(orient="records")  # list of dictionaries
        prediction = endpoint.predict(instances=records)
        return prediction

    def predict_from_csv(self, endpoint_display_name, csv_to_predict):
        """Make a prediction from a CSV file"""
        df = pd.read_csv(csv_to_predict)
        return self.predict_from_df(endpoint_display_name, df)

    def predict_from_dict(self, endpoint_display_name, data):

        # convert to list of dictionaries
        instances = [dict(zip(data.keys(), values)) for values in zip(*data.values())]
        endpoint = self.get_endpoint_by_display_name(endpoint_display_name)
        prediction = endpoint.predict(instances=instances)
        return prediction
