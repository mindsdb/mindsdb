[Vertex AI](https://cloud.google.com/vertex-ai) offers everything you need to build and use generative AI—from AI solutions, to Search and Conversation, to 130+ foundation models, to a unified AI platform.

## Setup

MindsDB provides the Vertex handler that enables you to connect Vertex AI models within MindsDB.


### AI Engine

Before creating a model, it is required to create an AI engine based on the provided handler.

> If you installed MindsDB locally, make sure to install all Vertex dependencies by running `pip install mindsdb[vertex]` or `pip install .[vertex]`.

You can create an Vertex engine using this command:

```sql
CREATE ML_ENGINE vertex FROM vertex
USING 
    project_id="vertex-1111",
    location="us-central1",
    staging_bucket="gs://my_staging_bucket",
    experiment="my-experiment",
    experiment_description="my experiment description",
    service_account_key_json = {
      "type": "service_account",
      "project_id": "vertex-1111",
      "private_key_id": "aaaaaaaaaa",
      "private_key": "---------BIG STRING WITH KEY-------\n",
      "client_email": "testvertexvaitest-11111.iam.gserviceaccount.com",
      "client_id": "1111111111111",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testbigquery%40bgtest-11111.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
   };
```

> Please note that you need to provide your service_account key here. It is also possible to pass your service account key as either a file path or a URL using the 'service_account_key_file` and `service_account_key_url` parameters respectively.

```sql
CREATE ML_ENGINE vertex FROM vertex
USING 
    project_id="vertex-1111",
    location="us-central1",
    staging_bucket="gs://my_staging_bucket",
    experiment="my-experiment",
    experiment_description="my experiment description",
    service_account_key_file="/home/user/MyProjects/vertex-1111.json";
```

```sql
CREATE ML_ENGINE vertex FROM vertex
USING 
    project_id="vertex-1111",
    location="us-central1",
    staging_bucket="gs://my_staging_bucket",
    experiment="my-experiment",
    experiment_description="my experiment description",
    service_account_key_url="https://storage.googleapis.com/vertex-1111.json?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=example%40example.iam.gserviceaccount.com%2F20220215%2Fus-central1%2Fstorage%2Fgoog4_request&X-Goog-Date=20220215T000000Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=abcd1234";
```


The name of the engine (here, `vertex`) should be used as a value for the `engine` parameter in the `USING` clause of the `CREATE MODEL` statement.

### AI Model

The [`CREATE MODEL`](/sql/create/model) statement is used to create, train, and deploy models within MindsDB.

```sql
CREATE MODEL mindsdb.vertex_anomaly_detection_model
PREDICT cut
USING 
    engine = 'vertex',
    model_name = 'diamonds_anomaly_detection',
    custom_model = True;
```

Where:

| Name              | Description                                                               |
|-------------------|---------------------------------------------------------------------------|
| `engine`          | It defines the Vertex engine.                                          |
| `model_name`      | It is used to provide the name of the model. |
| `custom_model`      | Is it custom model or not      |

## Usage

Once you have created an Vertex model, you can use it to make predictions.

```sql
SELECT t.cut, m.cut as anomaly
FROM files.vertex_anomaly_detection as t
JOIN mindsdb.vertex_anomaly_detection_model as m;
```
