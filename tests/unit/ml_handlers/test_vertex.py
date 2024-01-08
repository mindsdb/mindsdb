import time
import json

import pandas as pd

import pytest
from unittest.mock import Mock, patch
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest
from mindsdb.integrations.handlers.vertex_handler.vertex_client import VertexClient

path = "mindsdb.integrations.handlers.vertex_handler.vertex_client"


@pytest.fixture
def vertex_client():
    with patch(f"{path}.service_account.Credentials.from_service_account_file"), patch(f"{path}.aiplatform.init"):
        client = VertexClient("fake_path", "fake_project_id")
    return client


# Mocks
def mock_datasets():
    dataset_1 = Mock(display_name="Dataset1", name="ID1")
    dataset_2 = Mock(display_name="Dataset2", name="ID2")

    # Set concrete return values for attributes
    dataset_1.display_name = "Dataset1"
    dataset_1.name = "ID1"

    dataset_2.display_name = "Dataset2"
    dataset_2.name = "ID2"

    return [dataset_1, dataset_2]


def mock_endpoints():
    endpoint_1 = Mock(display_name="Endpoint1", name="EndpointID1")
    endpoint_2 = Mock(display_name="Endpoint2", name="EndpointID2")

    # Set concrete return values for attributes
    endpoint_1.display_name = "Endpoint1"
    endpoint_1.name = "EndpointID1"

    endpoint_2.display_name = "Endpoint2"
    endpoint_2.name = "EndpointID2"

    return [endpoint_1, endpoint_2]


def mock_models():
    model_1 = Mock(display_name="Model1", name="ModelID1")
    model_2 = Mock(display_name="Model2", name="ModelID2")

    # Set concrete return values for attributes
    model_1.display_name = "Model1"
    model_1.name = "ModelID1"

    model_2.display_name = "Model2"
    model_2.name = "ModelID2"

    return [model_1, model_2]


# Test of Vertex client class
def test_get_model_by_display_name(vertex_client):
    with patch(f"{path}.aiplatform.Model.list", return_value=mock_models()):
        model = vertex_client.get_model_by_display_name("Model1")
        assert model.display_name == "Model1"
        assert model.name == "ModelID1"


def test_get_endpoint_by_display_name(vertex_client):
    with patch(f"{path}.aiplatform.Endpoint.list", return_value=mock_endpoints()):
        endpoint = vertex_client.get_endpoint_by_display_name("Endpoint1")
        assert endpoint.display_name == "Endpoint1"
        assert endpoint.name == "EndpointID1"


def test_get_model_by_id(vertex_client):
    with patch(f"{path}.aiplatform.Model", return_value=mock_models()[0]):
        model = vertex_client.get_model_by_id("ModelID1")
        assert model.display_name == "Model1"
        assert model.name == "ModelID1"


def test_deploy_model(vertex_client):
    mock_model = mock_models()[0]
    with patch.object(mock_model, "deploy", return_value=mock_endpoints()[0]):
        endpoint = vertex_client.deploy_model(mock_model)
        assert endpoint.display_name == "Endpoint1"
        assert endpoint.name == "EndpointID1"


def test_predict_from_csv(vertex_client, mocker):
    mock_endpoint = mocker.MagicMock()
    mock_endpoint.predict.return_value = "CSV Predictions"

    mocker.patch(f"{path}.pd.read_csv", return_value=pd.DataFrame({"col1": ["data1", "data2"]}))
    mocker.patch(f"{path}.VertexClient.get_endpoint_by_display_name", return_value=mock_endpoint)

    predictions = vertex_client.predict_from_csv("Endpoint1", "path_to_csv")
    assert predictions == "CSV Predictions"


def test_predict_from_json(vertex_client, mocker):
    mock_endpoint = mocker.MagicMock()
    mock_endpoint.predict.return_value = "JSON Predictions"

    mock_open = mocker.mock_open(read_data='{"col1": ["data1", "data2"]}')
    mocker.patch("builtins.open", mock_open)

    mocker.patch(f"{path}.json.load", return_value={"col1": ["data1", "data2"]})
    mocker.patch(f"{path}.VertexClient.get_endpoint_by_display_name", return_value=mock_endpoint)

    """Make a prediction from a JSON file"""
    with open("path_to_json", "r") as f:
        data = json.load(f)

    predictions = vertex_client.predict_from_dict("Endpoint1", data)
    assert predictions == "JSON Predictions"


# Test of Vertex handler


class TestVertex(BaseExecutorTest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                if ret["STATUS"][0] == "complete":
                    done = True
                    break
                elif ret["STATUS"][0] == "error":
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_anomaly_detection_model(self, mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/vertex_anomaly_detection.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict cut
           using
            engine='vertex',
            model_name='diamonds_anomaly_detection',
            custom_model='True',
            vertex_args_path='tests/unit/ml_handlers/data/vertex_args.json',
            service_key_path='tests/unit/ml_handlers/data/vertex_service_key.json'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_regression_model(self, mock_handler):
        # create database
        self.run_sql("create database proj")
        df = pd.read_csv("tests/unit/ml_handlers/data/vertex_regression.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict actual_productivity
           using
            engine='vertex',
            model_name='productivity_regression',
            vertex_args_path='tests/unit/ml_handlers/data/vertex_args.json',
            service_key_path='tests/unit/ml_handlers/data/vertex_service_key.json'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_classification_model(self, mock_handler):
        # dataset, string values
        df = pd.read_csv("tests/unit/ml_handlers/data/vertex_classification.csv")
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create project
        self.run_sql("create database proj")

        # create predictor
        self.run_sql(
            """
           create model proj.modelx
           from pg (select * from df)
           predict Class
           using
            engine='vertex',
            model_name='fraud_detection',
            vertex_args_path='tests/unit/ml_handlers/data/vertex_args.json',
            service_key_path='tests/unit/ml_handlers/data/vertex_service_key.json'
        """
        )
        self.wait_predictor("proj", "modelx")

        # run predict
        ret = self.run_sql(
            """
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
        """
        )
        assert len(ret) == len(df)
