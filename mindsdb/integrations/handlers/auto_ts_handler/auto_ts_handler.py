import pandas as pd
from auto_ts import auto_timeseries as ATS
from typing import Optional
import dill

from mindsdb.integrations.libs.base import BaseMLEngine


class Auto_ts_Handler(BaseMLEngine):
    """
    Auto_ts handler class
    """
    name = 'auto_ts'

    @staticmethod
    def create_validation(self, args):
        """
        Create validation set from training set
        """
        score_type = ['rmse', 'normalized_rmse']
        frequency = ['B', 'C', 'D', 'W', 'M', 'SM', 'BM', 'CBM', 'MS',
                     'SMS', 'BMS', 'CBMS', 'Q', 'BQ', 'QS', 'BQS',
                     'A,Y', 'BA,BY', 'AS,YS', 'BAS,BYS', 'BH',
                     'H', 'T,min', 'S', 'L,ms', 'U,us', 'N']
        model = ['best', 'prophet', 'stats', 'ARIMA', 'SARIMAX', 'VAR', 'ML']

        if 'using' in args:
            args = args['using']

        if 'score_type' in args and args['score_type'] not in score_type:
            raise Exception(f"score_type must be one of {score_type}")

        if 'time_interval' in args and args['time_interval'] not in frequency:
            raise Exception(f"frequency must be one of {frequency}")

        if 'non_seasonal_pdq' in args and not (isinstance(args['non_seasonal_pdq'], tuple) or args['non_seasonal_pdq'] == 'None'):
            raise Exception("non_seasonal_pdq must be a tuple")

        if 'model_type' in args and args['model_type'] not in model:
            raise Exception(f"model_type must be one of {model}")

        if 'ts_column' not in args:
            raise Exception("Column containing time series must be defined")

        if 'target' not in args:
            raise Exception("target must be defined")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        """
        Creates Auto_ts model using the input df.
        """
        args = args['using']

        score_type = args.get('score_type', 'rmse')
        non_seasonal_pdq = args.get('non_seasonal_pdq', (3, 1, 3))
        time_interval = args.get('time_interval', 'M')
        seasonality = args.get('seasonality', False)
        model_type = args.get('model_type', 'best')
        cv = int(args.get('cv', 5))
        sep = args.get('sep', ',')
        seasonal_period = int(args.get('seasonal_period', None))

        model = ATS(score_type=score_type,
                    time_interval=time_interval,
                    non_seasonal_pdq=non_seasonal_pdq,
                    seasonality=seasonality,
                    seasonal_period=seasonal_period,
                    model_type=[model_type],
                    verbose=0)

        ts_column = args['ts_column']
        target = args['target']

        # Drop rows with null values
        df = df.dropna()
        model.fit(traindata=df, target=target, ts_column=ts_column, cv=cv, sep=sep)

        self.model_storage.json_set('args', args)
        self.model_storage.file_set('model', dill.dumps(model))

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> pd.DataFrame:
        """
        Predicts using the best Auto_ts model.
        """
        args = self.model_storage.json_get('args')
        target = args['target']
        model = dill.loads(self.model_storage.file_get('model'))
        df[f'{target}_preds'] = model.predict(testdata=df, model='best', simple=False)['yhat'].values
        return df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        """
        Describes the model.
        """
        args = self.model_storage.json_get('args')
        return pd.DataFrame(args, index=[0])
