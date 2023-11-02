from typing import List, Dict, Union
import numpy as np
import pandas as pd
from string import Formatter
import copy

import streamlit as st

task_template = {
    "parameters": {},
    "handlers": ["handler_name",],
    "use_cases": "blurb",
    "ml_task": "task name",
    "table": "table name",
    "model_inputs": "input columns",
    "targets": "target columns"
}

class Regression_Task:
    """
    A class to hold the base template for a regression ml task. Required arguments conform to PPE backend minimal
    requirements for this task.
    """

    def __init__(self,
                 table: str,
                 model_inputs: List[str],
                 targets: List[str],
                 **kwargs):
        self.tables = table
        self.model_inputs = model_inputs
        self.targets = targets
        self.__dict__.update(kwargs)

class Forecast_Task:
    """
    A class to hold the base template for a regression ml task. Required arguments conform to PPE backend minimal
    requirements for this task.
    """

    def __init__(self,
                 table: str,
                 model_inputs: List[str],
                 order: str,
                 groupby: List[str],
                 targets: List[str],
                 horizon: [str],
                 **kwargs):
        self.tables = table
        self.model_inputs = model_inputs
        self.order = order
        self.groupby = groupby
        self.targets = targets
        self.horizon = horizon
        self.__dict__.update(kwargs)


class Dataset:
    """
    A Dataset object holds a collection of ML task objects that specify what can be done with the data it contains. The
    Dataset object provides default handlers and connection parameters

    This architecture presumes that each ml-task is hosted on each handler.

    """
    def __init__(self,
                 name: str,
                 parameters: List[dict],
                 data_handlers: List[str],
                 ml_tasks: List[Union[Regression_Task, Forecast_Task]]
                 ):
        self.name = name
        self.parameters = parameters
        self.data_handlers = data_handlers
        self.ml_tasks = ml_tasks

        # Check for templates
        for ml_task in self.ml_tasks:
            if "use_case" not in vars(ml_task):
                raise Exception("No use case in ml task")


class MLHandler:
    def __init__(self,
                 name: str,
                 ml_tasks: List[Union[Regression_Task, Forecast_Task]],
                 apis: List[str],
                 train_templates: List[str],
                 predict_templates: List[str]
                 ):
        self.name = name
        self.ml_tasks = ml_tasks
        self.apis = apis
        self.train_templates = train_templates
        self.predict_templates = predict_templates

        # Check for templates
        for ml_task in self.ml_tasks:
            if "train_template" not in vars(ml_task):
                raise Exception("No train template in ml task")
            if "predict_template" not in vars(ml_task):
                raise Exception("No predict template in ml task")

    def format_template(self,
                        dataset_task: Union[Regression_Task, Forecast_Task],
                        train_task: Union[Regression_Task, Forecast_Task],
                        template_key: str
                        ):

        # update params dict
        params = copy.deepcopy(vars(train_task))
        params.update(vars(dataset_task))
        template = params[template_key]
        template_params = [x[1] for x in Formatter().parse(template)]

        for param in params.keys():
            if param not in template_params:
                del params[param]

        for param in template_params:
            if param not in params.keys():
                raise Exception("Required argument {} not supplied for {} template.".format(param, template_key))

        for key, val in params.items():
            if type(val) is list:
                params[key] = ', '.join(val)

        return params[template_key].format(**params)


class DataHandler:
    def __init__(self,
                 name: str,
                 dummy_params: dict,
                 apis: List[str],
                 templates: List[str]):
        self.name = name
        self.dummy_params = dummy_params
        self.apis = apis
        self.templates = templates

    def format_connect(self, dataset: Dataset, connect_select: int):
        return self.templates[0].format(**dataset.parameters[connect_select])  # TODO: map to api templates


"""******************************************************************************************************************"""

demo_dataset = Dataset(
    name="demo_dataset",
    parameters=[
        {"user": "demo_user", "password": "demo_password", "host": "3.220.66.106", "port": "5432", "database": "demo"},
        {"user": "demo_user", "password": "demo_password", "host": "3.220.66.106", "port": "5432", "database": "demo"}
    ],
    handlers=[['postgres'], ['postgres']],
    use_cases=["Predict Home Rental Prices", "Forecast Quarterly House Sales"],
    ml_tasks=["regression", "forecast"],
    tables=["example_db.demo_data.home_rentals", "example_db.demo_data.house_sales"],
    model_inputs=["*", "*"],
    # order=['',""],
    targets=["rental_price", "ma"],
)

lightwood_handler = MLHandler(
    name="lightwood",
    ml_tasks=["regression", "forecast"],
    apis=["MindsDB SQL"],
    train_templates=[
        "CREATE MODEL mindsdb.example_model FROM example_db (SELECT {model_input} FROM {table}) PREDICT {target};",
        "CREATE MODEL mindsdb.example_model FROM example_db (SELECT {model_input} FROM {table}) ORDER BY {order} GROUP BY {groupby} WINDOW {window} HORIZON {horizon};"
    ],
    predict_templates=[
        "SELECT t.{model_input}, m.{target} AS predicted_{target}, m.{target}_explain FROM {table} AS t JOIN mindsdb.example_model AS m LIMIT 10;",
        "SELECT m.{order} as date, m.{target} as forecast FROM mindsdb.example_model as m JOIN {table} as t WHERE t.{order} > LATEST LIMIT 4;"
    ]
)

postgres_handler = DataHandler(
    name="postgres",
    dummy_params={"user": "your_username", "password": "your_password", "host": "127.0.0.1", "port": "5432",
                  "database": "your_database"},
    apis=["MindsDB SQL"],
    templates=[
        'CREATE DATABASE example_db WITH ENGINE = "postgres", PARAMETERS = {{"user":"{user}","password":"{password}","host":"{host}","port":"{port}","database":"{database}"}};']
)

datasets = [demo_dataset]
data_handlers = [postgres_handler]
ml_handlers = [lightwood_handler]

"""******************************************************************************************************************"""


@st.cache_data
def build_tuples():
    df_list = []

    for dataset in datasets:  # Iterative through Datasets
        for i, task in enumerate(
                dataset.ml_tasks):  # Iterate through dataset's ML tasks (possibly multiple per dataset)
            for handler_name in dataset.handlers[i]:  # Iterate through tasks supported data handlers
                for data_handler in data_handlers:  # Iterate through data handler objects
                    if data_handler.name == handler_name:  # filter on data handler objects with dataset's listed handlers
                        for api in data_handler.apis:  # iterate through data handler objects supported api's
                            for ml_handler in ml_handlers:  # iterate through ml handler objects
                                if api in ml_handler.apis:  # filter on supported ml handler apis
                                    if task in ml_handler.ml_tasks:  # filter on supported ml handler tasks
                                        # record tuple information
                                        df_list.append(pd.DataFrame(
                                            {"ML Task": [task],
                                             "Use Case": [dataset.use_cases[i]],
                                             "Dataset": [dataset.name],
                                             "ML Handler": [ml_handler.name],
                                             "Dataset Handler": [data_handler.name],
                                             "API": [api],
                                             "Script": "Assembled Script"
                                             }
                                        )
                                        )
    return df_list


tuples_df = pd.concat(build_tuples()).reset_index(drop=True)

"""******************************************************************************************************************"""

for column in tuples_df.columns:
    if column not in st.session_state:
        st.session_state[column] = None


def filter():
    print("filter run")
    for column in tuples_df.columns:
        if st.session_state[column]:
            tuples_df.drop(index=tuples_df[tuples_df[column] != st.session_state[column]].index, inplace=True)


st.title('MindsDB CI Demo Generator')


def find_index(value, unique_df):
    indices = np.argwhere(value == unique_df)
    if len(indices) > 0:
        return int(indices[0])
    else:
        return None


filter()  # filter tuples_df before creating dropdowns.

labels = ['Select ML Task', 'Select Use Case', 'Select Dataset', 'Select ML Handler', 'Select Data Handler',
          'Select API']
for column, label in zip(tuples_df.columns, labels):
    st.session_state[column] = st.selectbox(label=label,
                                            options=tuples_df[column].unique(),
                                            index=find_index(st.session_state[column], tuples_df[column].unique()))

filter()  # filter tuples_df before creating dropdowns.


# st.write(st.session_state)

def reset():
    for column in tuples_df.columns:
        st.session_state[column] = None
    # st.write(st.session_state)


st.button(label="Reset", type="primary", on_click=reset)

# st.write(st.session_state)

"""******************************************************************************************************************"""

if len(tuples_df) == 1:
    # st.write("state 1")
    st.write(tuples_df["Dataset"])

    select_dataset = [dataset for dataset in datasets if dataset.name == tuples_df["Dataset"].iloc[0]][0]

    use_case_index = select_dataset.use_cases.index(tuples_df["Use Case"].iloc[0])

    select_data_handler = \
        [data_handler for data_handler in data_handlers if data_handler.name == tuples_df["Dataset Handler"].iloc[0]][
            0]

    select_ml_handler = \
        [ml_handler for ml_handler in ml_handlers if ml_handler.name == tuples_df["ML Handler"].iloc[0]][0]

    task_index = select_dataset.ml_tasks.index(tuples_df["ML Task"].iloc[0])

    connect_str = select_data_handler.format_connect(select_dataset, use_case_index)

    train_str = select_ml_handler.format_train(select_dataset, use_case_index, task_index)

    pred_str = select_ml_handler.format_predict(select_dataset, use_case_index, task_index)

    elements = [connect_str, train_str, pred_str]

    script_str = "\n".join(elements)

    st.write('Your script:')
    for element in elements:
        st.write(element)
elif len(tuples_df) == 0:
    # st.write("state 0")
    reset()
else:
    # st.write("state >1")
    st.write('Select more options to generate script...')
    st.write(tuples_df)
