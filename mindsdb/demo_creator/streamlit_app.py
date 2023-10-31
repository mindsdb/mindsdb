from typing import List, Dict
import numpy as np
import pandas as pd

import streamlit as st


class Dataset:
    def __init__(self,
                 name: str,
                 parameters: List[dict],
                 handlers: List[List[str]],
                 use_cases: List[str],
                 ml_tasks: List[str],
                 tables: List[str],
                 model_inputs: List[str],
                 targets: List[str]):
        self.name = name
        self.parameters = parameters
        self.handlers = handlers
        self.use_cases = use_cases
        self.ml_tasks = ml_tasks
        self.tables = tables
        self.model_inputs = model_inputs
        self.targets = targets


class MLHandler:
    def __init__(self,
                 name: str,
                 ml_tasks: List[str],
                 apis: List[str],
                 train_templates: List[str],
                 predict_templates: List[str]
                 ):
        self.name = name
        self.ml_tasks = ml_tasks
        self.apis = apis
        self.train_templates = train_templates
        self.predict_templates = predict_templates

    def format_train(self, dataset: Dataset, dataset_task: int, train_task: int):
        return self.train_templates[train_task].format(
            model_input=dataset.model_inputs[dataset_task],
            target=dataset.targets[dataset_task],
            table=dataset.tables[dataset_task]
        )

    def format_predict(self, dataset: Dataset, dataset_task: int, train_task: int):
        return self.predict_templates[train_task].format(
            model_input=dataset.model_inputs[dataset_task],
            target=dataset.targets[dataset_task],
            table=dataset.tables[dataset_task]
        )


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
        return self.templates[0].format(**dataset.parameters[connect_select])  #TODO: map to api templates


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
