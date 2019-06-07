TRANSACTION_LEARN = 'learn'
TRANSACTION_PREDICT = 'predict'
TRANSACTION_NORMAL_SELECT = 'normal_select'
TRANSACTION_NORMAL_MODIFY = 'normal_modify'
TRANSACTION_BAD_QUERY = 'bad_query'
TRANSACTION_DROP_MODEL ='drop_model'

STOP_TRAINING = 'stop_training'
KILL_TRAINING = 'kill_training'

KEY_NO_GROUP_BY = 'ALL_ROWS_NO_GROUP_BY'


class DATA_SUBTYPES:
    # Numeric
    INT = 'Int'
    FLOAT = 'Float'
    BINARY = 'Binary' # Should we have this ?

    # DATETIME
    DATE = 'Date' # YYYY-MM-DD
    TIMESTAMP = 'Timestamp' # YYYY-MM-DD hh:mm:ss or 1852362464

    # CATEGORICAL
    SINGLE = 'Binary Category'
    MULTIPLE = 'Category'

    # FILE_PATH
    IMAGE = 'Image'
    VIDEO = 'Video'
    AUDIO = 'Audio'

    # URL
    # How do we detect the tpye here... maybe setup async download for random sample an stats ?

    # SEQUENTIAL
    TEXT = 'Text'
    ARRAY = 'Array' # Do we even want to support arrays / structs / nested ... etc ?

class DATA_TYPES:
    NUMERIC = 'Numeric'
    DATE = 'Date'
    CATEGORICAL = 'Categorical'
    FILE_PATH = 'File Path'
    URL = 'Url'
    SEQUENTIAL = 'Sequential'

class DATA_TYPES_SUBTYPES:
    subtypes = {
        DATA_TYPES.NUMERIC: (DATA_SUBTYPES.INT, DATA_SUBTYPES.FLOAT, DATA_SUBTYPES.BINARY)
        ,DATA_TYPES.DATE:(DATA_SUBTYPES.DATE, DATA_SUBTYPES.TIMESTAMP)
        ,DATA_TYPES.CATEGORICAL:(DATA_SUBTYPES.SINGLE, DATA_SUBTYPES.MULTIPLE)
        ,DATA_TYPES.FILE_PATH:(DATA_SUBTYPES.IMAGE, DATA_SUBTYPES.VIDEO, DATA_SUBTYPES.AUDIO)
        ,DATA_TYPES.URL:()
        ,DATA_TYPES.SEQUENTIAL:(DATA_SUBTYPES.TEXT, DATA_SUBTYPES.ARRAY)
    }


class ORDER_BY_KEYS:
    COLUMN = 0
    ASCENDING_VALUE = 1

PHASE_DATA_EXTRACTOR = 1
PHASE_STATS_GENERATOR = 2
PHASE_MODEL_INTERFACE = 3
PHASE_MODEL_ANALYZER = 4

MODEL_STATUS_TRAINED = "Trained"
MODEL_STATUS_PREPARING = "Preparing"
MODEL_STATUS_DATA_ANALYSIS = "Data Analysis"
MODEL_STATUS_TRAINING= "Training"
MODEL_STATUS_ANALYZING = "Analyzing"
MODEL_STATUS_ERROR = "Error"

WORD_SEPARATORS = [',', "\t", ' ']

DEBUG_LOG_LEVEL = 10
INFO_LOG_LEVEL = 20
WARNING_LOG_LEVEL = 30
ERROR_LOG_LEVEL = 40
NO_LOGS_LOG_LEVEL = 50
