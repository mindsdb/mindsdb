"""
    Tasks queue allows to limit load by ordering tasks through a queue.
    Current implementation use Redis as backend for the queue. To run MindsDB with tasks queue need:
    1. config mindsdb to use tasks queue by one of:
        - fill 'ml_task_queue' key in config.json:
            {
                "ml_task_queue": {
                    "type": "redis",    // required
                    "host": "...",
                    "port": "...",
                    "db": "...",
                    "username": "...",
                    "password": "..."
                }
            }
        - or set env vars:
            MINDSDB_ML_QUEUE_TYPE=redis  # required
            MINDSDB_ML_QUEUE_HOST=...
            MINDSDB_ML_QUEUE_PORT=...
            MINDSDB_ML_QUEUE_DB=...
            MINDSDB_ML_QUEUE_USERNAME=...
            MINDSDB_ML_QUEUE_PASSWORD=...
    2. run mindsdb with arg --ml_task_queue_consumer

    In redis there is two types of entities used: streams (for distributing tasks) and regular
    key-value storage with ttl (to transfer dataframes and some other data). Dataframes are not
    transfer via streams to make stream messages lightweight.

    Taks queue may work in single instnace to limit load on it, ot it may work in distributed
    system. In that case mindsdb may be splitted into two modules: parser/planner/executioner (PPE)
    and ML.

  ┌─────────────┐ ┌─────────────┐
  │             │ │             │
  │ MindsDB PPE │ │ MindsDB PPE │
  │             │ │             │
  └───────────┬─┘ └─┬─────┬─▲───┘
              │     │     │ │
             ┌▼─────▼┐    │ │
             │       │  ┌─▼─┴─────────┐
             │ Queue │  │             │
             │       │  │  Cache      │
             ├───────┤  │             │
             │ Task  │  ├─────────────┤
             ├───────┤  │  Dataframe  │
             │ Task  │  ├─────────────┤
             ├───────┤  │  Status     │
             │ Task  │  └─┬─▲─────────┘
             └┬─────┬┘    │ │
              │     │     │ │
              │     │     │ │
  ┌───────────▼─┐ ┌─▼─────▼─┴───┐
  │             │ │             │
  │ MindsDB ML  │ │ MindsDB ML  │
  │             │ │             │
  └─────────────┘ └─────────────┘
"""