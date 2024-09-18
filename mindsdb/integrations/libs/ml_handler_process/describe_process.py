import importlib
from textwrap import dedent
from types import ModuleType
from typing import Optional, Union

from pandas import DataFrame

import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage
from mindsdb.interfaces.model.model_controller import ModelController


def get_module_import_error_str(module: ModuleType) -> str:
    '''Make a str with human-readable module import error message

    Atrs:
        module (ModuleType): module with import error

    Returns:
        str: error message
    '''
    is_cloud = Config().get('cloud', False)

    msg = dedent(f'''\
        ML engine '{module.name}' cannot be used. Reason is:
            {module.import_error}
    ''')

    if is_cloud is False:
        msg += '\n'
        msg += dedent(f'''\
            If error is related to missing dependencies, then try to run command in shell and restart mindsdb:
                pip install mindsdb[{module.name}]
        ''')

    return msg


def describe_process(integration_id: int, attribute: Optional[Union[str, list]],
                     model_id: int, module_path: str) -> DataFrame:
    '''get a model description

    Args:
        model_id (int): id of the model
        integration_id (int): id of the integration
        attribute (Optional[Union[str, list]]): attribute, or list model attributes to describe
        module_path: (str): path integration module

    Returns:
        DataFrame: usually 1-row dataframe with model description
    '''
    module = importlib.import_module(module_path)

    handlerStorage = HandlerStorage(integration_id)
    modelStorage = ModelStorage(model_id)

    model_record = db.Predictor.query.get(model_id)
    if model_record is None:
        return DataFrame(['The model does not exist'], columns=['error'])

    if isinstance(attribute, str) and attribute.lower() == 'import_error':
        return DataFrame([get_module_import_error_str(module)], columns=['error'])

    if attribute is not None:
        if module.import_error is not None:
            return DataFrame([get_module_import_error_str(module)], columns=['error'])

        try:
            ml_handler = module.Handler(
                engine_storage=handlerStorage,
                model_storage=modelStorage
            )
            return ml_handler.describe(attribute)
        except NotImplementedError:
            return DataFrame()
        except Exception as e:
            return DataFrame(
                [f'{e.__class__.__name__}: {e}'],
                columns=['error']
            )
    else:
        model_info = ModelController.get_model_info(model_record)

        attrs_df = DataFrame()
        if module.import_error is not None:
            attrs_df = DataFrame(['import_error'], columns=['error'])
            model_error = model_info['ERROR'][0] or '-'
            model_info['ERROR'][0] = 'ML engine error:\n\n'
            model_info['ERROR'][0] += get_module_import_error_str(module)
            model_info['ERROR'][0] += '\nModel error:\n\n'
            model_info['ERROR'][0] += model_error
        else:
            try:
                ml_handler = module.Handler(
                    engine_storage=handlerStorage,
                    model_storage=modelStorage
                )
                attrs_df = ml_handler.describe(attribute)
            except NotImplementedError:
                pass
            except Exception as e:
                model_error = model_info['ERROR'][0] or '-'
                model_info['ERROR'][0] = 'ML engine error:\n\n'
                model_info['ERROR'][0] += f'{e.__class__.__name__}: {e}\n'
                model_info['ERROR'][0] += '\nModel error:\n\n'
                model_info['ERROR'][0] += model_error

        attributes = []
        if len(attrs_df) > 0 and len(attrs_df.columns) > 0:
            attributes = list(attrs_df[attrs_df.columns[0]])
            if len(attributes) == 1 and isinstance(attributes[0], list):
                # first cell already has a list
                attributes = attributes[0]

        model_info.insert(0, 'TABLES', [attributes])
        return model_info
