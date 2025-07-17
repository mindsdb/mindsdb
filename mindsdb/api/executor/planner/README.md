# Planner


## How to use

**Initialize planner**

```python
from mindsdb.api.executor.planner import query_planner

# all parameters are optional
planner = query_planner.QueryPlanner(
    ast_query, # query as AST-tree
    integrations=['mysql'], # list of available integrations
    predictor_namespace='mindsdb', # name of namespace to lookup for predictors
    default_namespace='mindsdb', # if namespace is not set in query default namespace will be used
    predictor_metadata={ # information about predictors
        'tp3': { # name of predictor
           'timeseries': True, # is timeseries predictor
           'order_by_column': 'pickup_hour', # timeseries column 
           'group_by_columns': ['day', 'type'], # columns for partition (only for timeseries) 
           'window': 10 # windows size (only for timeseries) 
        }
    }
)

```
Detailed description of timeseries predictor: [https://docs.mindsdb.com/sql/create/predictor/]


**Plan of prepared statement**

Planner can be used in case of query with parameters: query is not complete and can't be executed. 
But it is possible to get list of columns and parameters from query.

```python
for step in planner.prepare_steps(ast_query):
    data = do_execute_step(step)
    step.set_result(data)

statement_info = planner.get_statement_info()

# list of columns
print(statement_info['columns'])

# list of parameters
print(statement_info['parameters'])
```

At the moment this functionality is used only in COM_STMT_PREPARE command of mysql binary protocol.

**Plan of execution**

```python

# if prepare_steps was executed we need pass params.
# otherwise, params=None
for step in planner.execute_steps(params):
    data = do_execute_step(step)
    step.set_result(data)
```

Query result data will be on output of the last step.

**Alternative way of execution**

At the moment execution plan doesn't dependent from results of previous steps. 
But this behavior can be changed in the future.

With the current behavior that it is possible to get plan of query as list:

```python
from mindsdb.api.executor.planner import plan_query

plan = plan_query(
    ast_query,
    integrations=['mysql'], 
    predictor_namespace='mindsdb', 
    default_namespace='mindsdb', 
    predictor_metadata={
        'tp3': {
           'timeseries': False, 
        }
    }
)
# list of steps
print(plan.steps)

```

## Architecture

Planner is analysing AST-query and return sequence of steps that is needed to execute to perform query.

Steps are defined in planner/steps.py. Steps can reference to future result of previous step (using class Result in planner/step_results.py)

Query planner consists from 2 different planner:

1. For prepare statement is class PreparedStatementPlanner in query_prepare.py

2. For execution is class QueryPlanner in query_panner.py
The most complex part of planner is planning of join table with timeseries predictor. Logic briefly:
- extract query for integration (without predictor)
- select all possible values of group fields (in scope of query)
- for every value of group field
  - select part of data according to filters and size of window
- join all data in one dataframe
- pass it to predictor input
- join predictor results with data before prediction 
