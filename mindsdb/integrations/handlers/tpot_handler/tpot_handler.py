from typing import Optional
import dill
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from typing import Dict, Optional
from type_infer.infer import infer_types
from tpot import TPOTClassifier, TPOTRegressor


class TPOTHandler(BaseMLEngine):
    name = "TPOT"
    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if args is None:
            args = {}
        
        
        self.target=target

        target_dtype=infer_types(df,0).to_dict()["dtypes"][self.target]


        if target_dtype in ['binary','categorical','tags']:
            model = TPOTClassifier(generations=args.get('generations', 10),
                                       population_size=args.get('population_size', 100),
                                       verbosity=2,
                                       max_time_mins=args.get('max_time_mins', None),
                                       n_jobs=args.get('n_jobs', -1))
            

        elif target_dtype in ['integer','float','quantity'] :
            model = TPOTRegressor(generations=args.get('generations', 10),
                                      population_size=args.get('population_size', 100),
                                      verbosity=2,
                                      max_time_mins=args.get('max_time_mins', None),
                                      n_jobs=args.get('n_jobs', -1))
        else:
            print('Unexpected error Occur')

        if df is not None:
            model.fit(df.drop(columns=[self.target]), df[self.target])
        self.model_storage.json_set('args', args)
        self.model_storage.file_set('model', dill.dumps(model.fitted_pipeline_))
        

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        model=dill.loads(self.model_storage.file_get("model"))
        args=self.model_storage.json_get('args')
        target=args.get("target","Not Found")
        results=pd.DataFrame(model.predict(df),columns=[target])
        
        return results
