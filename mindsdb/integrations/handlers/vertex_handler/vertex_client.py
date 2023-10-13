from google.cloud import aiplatform
from google.oauth2 import service_account
import pandas as pd
import json

PROJECT_ID = "mindsdb-401709"  # You'll need to first create a project in the console and enable vertex AI
PATH_TO_SERVICE_ACCOUNT_JSON = "service_key.json"  # Then you can generate this key in the console for the project


class VertexClient:
    """A class to interact with Vertex AI"""

    def __init__(self, credentials_path, project_id):
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        aiplatform.init(
            project=project_id,
            location="us-central1",
            staging_bucket="gs://my_staging_bucket",
            credentials=credentials,
            # the name of the experiment to use to track
            # logged metrics and parameters
            experiment="my-experiment",
            # description of the experiment above
            experiment_description="my experiment description",
        )

    def print_datasets(self):
        """Print all datasets and dataset ids in the project"""
        for dataset in aiplatform.TabularDataset.list():
            print(f"Dataset display name: {dataset.display_name}, ID: {dataset.name}")

    def print_models(self):
        """Print all model names and model ids in the project"""
        for model in aiplatform.Model.list():
            print(f"Model display name: {model.display_name}, ID: {model.name}")

    def print_endpoints(self):
        """Print all endpoints and endpoint ids in the project"""
        for endpoint in aiplatform.Endpoint.list():
            print(f"Endpoint display name: {endpoint.display_name}, ID: {endpoint.name}")

    def get_model_by_display_name(self, display_name):
        """Get a model by its display name"""
        try:
            return aiplatform.Model.list(filter=f"display_name={display_name}")[0]
        except IndexError:
            print(f"Model with display name {display_name} not found")

    def get_endpoint_by_display_name(self, display_name):
        """Get an endpoint by its display name"""
        try:
            return aiplatform.Endpoint.list(filter=f"display_name={display_name}")[0]
        except IndexError:
            print(f"Endpoint with display name {display_name} not found")

    def get_model_by_id(self, model_id):
        """Get a model by its ID"""
        try:
            return aiplatform.Model(model_name=model_id)
        except IndexError:
            print(f"Model with ID {model_id} not found")

    def deploy_model(self, model):
        """Deploy a model to an endpoint - long runtime"""
        endpoint = model.deploy()
        return endpoint

    def predict_from_df(self, endpoint_display_name, df):
        """Make a prediction from a Pandas dataframe"""
        endpoint = self.get_endpoint_by_display_name(endpoint_display_name)
        records = df.astype(str).to_dict(orient="records")  # list of dictionaries
        prediction = endpoint.predict(instances=records)
        return prediction

    def predict_from_csv(self, endpoint_display_name, csv_to_predict):
        """Make a prediction from a CSV file"""
        df = pd.read_csv(csv_to_predict)
        return self.predict_from_df(endpoint_display_name, df)

    def predict_from_json(self, endpoint_display_name, json_to_predict):
        """Make a prediction from a JSON file"""
        with open(json_to_predict, "r") as f:
            instances = json.load(f)

        # convert to list of dictionaries
        instances = [dict(zip(instances.keys(), values)) for values in zip(*instances.values())]
        endpoint = self.get_endpoint_by_display_name(endpoint_display_name)
        prediction = endpoint.predict(instances=instances)
        return prediction
