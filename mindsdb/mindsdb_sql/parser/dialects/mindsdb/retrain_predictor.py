from mindsdb.mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.mindsdb_sql.parser.utils import indent
from .create_predictor import CreatePredictorBase

class RetrainPredictor(CreatePredictorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._command = 'RETRAIN'