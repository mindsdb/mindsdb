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
    project_id="mindsdb-401709",
    location="us-central1",
    staging_bucket="gs://my_staging_bucket",
    experiment="my-experiment",
    experiment_description="my experiment description",
    service_account = {
      ...paste service account keys here
    };
```

> Please note that you need to provide your service_account key's here. 

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
