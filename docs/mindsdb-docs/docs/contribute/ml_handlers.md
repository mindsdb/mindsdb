# Building a Machine Learning Handler

In this section, you'll find how to create new machine learning (ML) handlers within MindsDB.

## Prerequisite

You should have the latest staging version of the MindsDB repository installed locally. Follow [this guide](/contribute/install/) to learn how to install MindsDB for development.

## What are Machine Learning Handlers?

ML handlers act as a bridge to any ML framework. You use ML handlers to create ML engines using [the `CREATE ML_ENGINE` command](/sql/create/ml_engine/). So you can expose ML models from any supported ML engine as an AI table.

## ML Handlers in the MindsDB Repository

The source code for ML handlers is located in the main MindsDB repository under the [/integrations](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations) directory.

```
integrations                      # Contains ML handlers source codes
├─ handlers/                           # Each ML engine has its own handler directory
│  ├─ huggingface_handler/                 # HuggingFace code
│  ├─ lightwood_handler/                   # Lightwood code
│  ├─  .../                                # Other handlers
├─ libs/                               # Handler libraries directory
│  ├─ base.py                              # Each ML handler class inherits from the BaseMLEngine base class
└─ utilities                           # Handler utility directory
│  ├─ install.py                           # Script that installs all handler dependencies
```

## Creating a Machine Learning Handler

You can create your own ML handler within MindsDB by inheriting from [the `BaseMLEngine` class](https://github.com/mindsdb/mindsdb/blob/3d9090acb0b8b3b0e2a96e2c93dad436f5ebef90/mindsdb/integrations/libs/base.py#L123).

By providing implementation for some or all of the methods contained in the `BaseMLEngine` class, you can connect with the machine learning library or framework of your choice.

### Core Methods

Apart from the `__init__()` method, there are five methods, of which two must be implemented. Let's review the purpose of each method.

| Method            | Purpose                                                                              |
|-------------------|--------------------------------------------------------------------------------------|
| `create()`        | It creates a model inside the engine registry.                                       |
| `predict()`       | It calls a model and returns prediction data.                                        |
| `update()`        | Optional. It updates an existing model without resetting its internal structure.     |
| `describe()`      | Optional. It provides global models insights.                                        |
| `create_engine()` | Optional. It connects with external sources, such as REST API.                       |

### Implementing Mandatory Methods

Here are the methods that must be implemented while inheriting from the `BaseMLEngine` class:

* [The `create()` method](https://github.com/mindsdb/mindsdb/blob/3d9090acb0b8b3b0e2a96e2c93dad436f5ebef90/mindsdb/integrations/libs/base.py#L151) saves a model inside the engine registry for later usage.

```py
def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Saves a model inside the engine registry for later usage.
        Normally, an input dataframe is required to train the model.
        However, some integrations may merely require registering the model instead of training, in which case `df` can be omitted.
        Any other arguments required to register the model can be passed in an `args` dictionary.
        """
```

* [The `predict()` method](https://github.com/mindsdb/mindsdb/blob/3d9090acb0b8b3b0e2a96e2c93dad436f5ebef90/mindsdb/integrations/libs/base.py#L162) calls a model with an input dataframe and optionally, arguments to modify model's behaviour. This method returns a dataframe with the predicted values.

```py
def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Calls a model with some input dataframe `df`, and optionally some arguments `args` that may modify the model behavior.
        The expected output is a dataframe with the predicted values in the target-named column.
        Additional columns can be present, and will be considered row-wise explanations if their names finish with `_explain`.
        """
```

### Implementing Optional Methods

Here are the optional methods that you can implement alongside the mandatory ones if your ML framework allows it:

* [The `update()` method](https://github.com/mindsdb/mindsdb/blob/3d9090acb0b8b3b0e2a96e2c93dad436f5ebef90/mindsdb/integrations/libs/base.py#L171) is used to update, fine-tune, or adjust an existing model without resetting its internal state.

```py
def update(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Optional.
        Used to update/fine-tune/adjust a pre-existing model without resetting its internal state (e.g. weights).
        Availability will depend on underlying integration support, as not all ML models can be partially updated.
        """
```

* [The `describe()` method](https://github.com/mindsdb/mindsdb/blob/3d9090acb0b8b3b0e2a96e2c93dad436f5ebef90/mindsdb/integrations/libs/base.py#L181) provides global models insights, such as framework-level parameters used in training.

```py
def describe(self, key: Optional[str] = None) -> pd.DataFrame:
        """
        Optional.
        When called, this method provides global model insights, e.g. framework-level parameters used in training.
        """
```

* [The `create_engine()` method](https://github.com/mindsdb/mindsdb/blob/3d9090acb0b8b3b0e2a96e2c93dad436f5ebef90/mindsdb/integrations/libs/base.py#L189) is used to connect with the external sources, such as REST API.

```py
def create_engine(self, connection_args: dict):
        """
        Optional.
        Used to connect with external sources (e.g. a REST API) that the engine will require to use any other methods.
        """
```

## Check out our Machine Learning Handlers!

To see some ML handlers that are currently in use, we encourage you to check out the following ML handlers inside the MindsDB repository:

* [Lightwood](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/lightwood_handler)
* [HuggingFace](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/huggingface_handler)
* [Ludwig](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/ludwig_handler)

And here are [all the handlers available in the MindsDB repository](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers).
