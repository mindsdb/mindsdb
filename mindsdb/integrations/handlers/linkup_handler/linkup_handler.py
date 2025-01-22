from linkup import LinkupClient
from mindsdb.integrations.libs.base import BaseMLEngine
from typing import Dict, Optional
import pandas as pd


class LinkupHandler(BaseMLEngine):

    name: str = "Linkup Search Tool"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if args is None:
            raise ValueError("Configurations arguments have to be specified")
        self.model_storage.json_set('args', args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        stored_args = self.model_storage.json_get('args')
        if stored_args is None:
            raise ValueError("No stored arguments found. Ensure 'create' method is called before 'predict'.")
        
        api_key = stored_args['using']['api_key']

        self.client = LinkupClient(api_key=api_key)

        results = []
        for query in df['question']:
            try:
                response = self.client.search(
                    query=query,
                    depth=stored_args['using']['depth'],
                    output_type=stored_args['using']['output_type']
                )
                results.append({'question': query, 'answer': response})
            except Exception as e:
                results.append({
                    'question': query,
                    'answer': None,
                    'error': str(e),
                    'error_explain': f"{str(e)}"
                })
        return pd.DataFrame(results)
