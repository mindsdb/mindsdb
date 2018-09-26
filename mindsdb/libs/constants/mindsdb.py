MDB_VERSION = "0.5.9"

DEFAULT_ORDER_BY_TYPE = 'DESC'
TRANSACTION_LEARN = 'learn'
TRANSACTION_PREDICT = 'predict'
TRANSACTION_CLUSTER = 'cluster'
TRANSACTION_NORMAL_SELECT = 'normal_select'
TRANSACTION_NORMAL_MODIFY = 'normal_modify'
TRANSACTION_BAD_QUERY = 'bad_query'
TRANSACTION_SHOW_DATASOURCES = 'show_datasources'
TRANSACTION_DROP_MODEL ='drop_model'

STOP_TRAINING = 'stop_training'
KILL_TRAINING = 'kill_training'

KEY_COLUMNS = 'columns'
KEY_STATS = 'stats'
KEY_MODEL_METADATA = 'model_metadata'
KEY_MODEL_NAME = 'model_name'
KEY_SUBMODEL_NAME = 'submodel_name'
KEY_MODEL_QUERY = 'model_query'
KEY_MODEL_TEST_QUERY = 'model_test_query'
KEY_MODEL_GROUP_BY = 'model_group_by'
KEY_MODEL_ORDER_BY = 'model_order_by'
KEY_MODEL_PREDICT_COLUMNS = 'model_predict_columns'
KEY_MODEL_WHEN_CONDITIONS = 'model_when_conditions'
KEY_MODEL_UPDATE_FREQUENCY = 'model_upate_frequency'
KEY_MODEL_CACHE_RESULTS = 'model_cache_results'
KEY_MODEL_CACHE = 'model_cache'
KEY_COLUMN_MAP = 'column_map'
KEY_MODEL_MODEL_TYPE = 'predictor'
KEY_MODEL_PREDICT_FROM_TABLE = 'model_predict_from_table'
KEY_CONFIDENCE ='prediction_confidence'

KEY_TRANSACTION_TYPE = 'transaction_type'

LEARNING_RATE_INDEX = 0
EPOCHS_INDEX = 1

KEY_NO_GROUP_BY = 'ALL_ROWS_NO_GROUP_BY'

KEY_START = 'start'
KEY_END = 'end'
KEY_METADATA = 'metadata'
KEY_STATUS = 'status'

FULL_TEXT_ENCODING_EXTRA_LENGTH = 4 #(len)[is_start, is_end, unfrequent_word, is_null]
TEXT_ENCODING_EXTRA_LENGTH = 2 #(len)[unfrequent_word, is_null]

ALL_INDEXES_LIST = ['*']

class DATA_TYPES:
    FULL_TEXT = 'FullText'
    NUMERIC = 'Numeric'
    DATE = 'Date'
    TEXT = 'Text'

class SAMPLE_TYPES:
    TEST_SET = 'test'
    TRAIN_SET = 'train'
    PREDICT_SET = 'predict'

class SAMPLER_MODES:
    LEARN = 'learn'
    PREDICT = 'predict'
    DEFAULT = None

class KEYS:
    TEST_SET = 'test'
    TRAIN_SET = 'train'
    PREDICT_SET = 'predict'

    ALL_SET = 'all'
    X ='x'
    Y ='y'
    DATA_TYPE = 'DataType'
    TS_ENCODED_ROW = 'TS_ENCODED_ROW'
    TS_FLAT_ROW = 'TS_FLAT_ROW'

class PREDICTORS:
    DEFAULT = 'default'
    CUSTOM = 'custom'

class FRAMEWORKS:
    PYTORCH = 'pytorch'
    TENSORFLOW = 'tensorflow'
    KERAS = 'keras'


PHASE_START = 0
PHASE_END = 1000
PHASE_DATA_EXTRACTION = 1
PHASE_DATA_STATS = 2
PHASE_DATA_VECTORIZATION = 3
PHASE_DATA_ENCODER = 4
PHASE_MODEL_TRAINER = 5
PHASE_PREDICTION = 5
PHASE_DATA_DEVECTORIZATION = 6

MODEL_STATUS_TRAINED = "Trained"
MODEL_STATUS_PREPARING = "Preparing"
MODEL_STATUS_TRAINING= "Training"
MODEL_STATUS_ANALYZING = "Analyzing"
MODEL_STATUS_ERROR = "Error"

WORD_SEPARATORS = [',', "\t", ' ']

SAMPLER_FLAT_TENSORS = 'flat_vectors'
SAMPLER_COLUMN_TENSORS = 'column_tensors'

MODEL_GROUP_BY_DEAFAULT_LIMIT = 80

# Types of queries

# Creating a data model
'''
CREATE/UPDATE MODEL FROM (
    select
        *
    from diamonds
    
) AS diamond_pricing

PREDICT price
'''



'''
-- GET ALL DIAMONDS AND FILL IN THE PRICE IF ITS NULL
SELECT * FROM diamond PREDICT price;
'''

# Creating a data model
'''
CREATE/UPDATE MODEL FROM (
    select

        p.patient_id,
        p.patient_current_age,
        p.patient_sex,
        p.care_plan,
        cc.chronic_condition,
        v.visit_diagnosis_cd,
        v.start_time
        v.end_date

    from patient p
    left join patient_visit v
        on p.patient_id = v.patient_id
    left join patient_chronic_condition cc
        on p.patient_id = cc.patient_id
        and cc.date >= v.start_date
        and cc.data <= v.end_date

) AS patient_history
-- OPTIONAL:
GROUP BY patient_id
ORDER BY start_date
PREDICT *, CLUSTERS
UPDATE (hourly, weekly, monthly)
CACHE (true, false)
PREDICTOR pytorch.default_
'''


# possible queries
'''
-- PREDICT A SINGLE COLUMN
SELECT * FROM patient_history PREDICT patient_sex;
-- PREDICT ONE OR ALL CALUMNS IN A FUTURE TIME FOR A GIVEN PATIENT 230
SELECT * FROM patient_history PREDICT * WHEN start_time > NOW() and patient_id = 230;
SELECT * FROM patient_history PREDICT care_plan WHEN start_time > NOW();
-- GET CLUSTERS that patients belong to using patient_history, clusters column is a json
SELECT *, CLUSTERS FROM patient PREDICT 10 CLUSTERS USING patient_history;
-- GET CLUSTERS that patients belong at any given time in their history, clusters column is a json
SELECT *, CLUSTERS[0].id, CLUSTERS[0].confidence  FROM patient_history PREDICT 10 CLUSTERS;
-- GET 200 most alike patients to patient_id=100, ALIKE column is a percentage
SELECT *, ALIKE FROM patient PREDICT 200 ALIKE patient_id=100 USING patient_history;
-- PREDICT patient_sex on a differnt patient history given what was learnt from patient_history
SELECT * FROM other_patient_history PREDICT patient_sex USING patient_history;

'''



# simulate given a trained model

"""
WITH (
    SELECT
        15 as patient_current_age,
        'plus' as care_plan,
        'diabetes' as chronic_condition
) as test
SELECT * from test PREDICT patient_sex USING patient_history
"""



# e-commerce
"""
CREATE/UPDATE MODEL FROM (
    select

        c.customer_id,
        c.customer_age,
        c.customer_gender,
        s.product_id,
        p.produc_type,
        p.product_price,
        s.discount,
        s.sold_date,
        s.returned

    from customer c
    left join sales s
        on c.customer_id = s.customer_id
    left join product p
        on p.product_id = s.product_id

) AS customer_history
-- OPTIONAL:
GROUP BY customer_id
ORDER BY sold_date
PREDICT *
CALCULATE_CLUSTERS (true, false)
UPDATE (hourly, weekly, monthly)
CACHE (true, false)
"""


# possible queries
"""
-- PREDICT IF A sold product will be returned
SELECT * FROM customer_history PREDICT returned;
-- GET THE MOST LIKELY PRODUCTS THAT a given customer will buy
SELECT product_id FROM customer_history PREDICT product_id WHEN sold_date > NOW();
-- GET 100 SIMILAR CUSTOMERS to customer 581
SELECT * FROM customer PREDICT 100 ALIKE customer_id = 581 USING customer_history;
"""

