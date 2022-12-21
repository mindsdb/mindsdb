import os

# required env vars:
#   MINDSDB_DB_CON
#   S3_BUCKET

os.environ['MINDSDB_DB_CON'] = '<>'
os.environ['S3_BUCKET'] = '<>'

from mindsdb.integrations.handlers_wrapper.ray_server import MLHandlerRay

import mindsdb.interfaces.storage.db as db

db.init()

entrypoint = MLHandlerRay.options(
    autoscaling_config={"min_replicas": 1, "max_replicas": 11*4},
    # ray_actor_options={'num_gpus': 0.25, 'num_cpus': 1},
    ray_actor_options={'num_gpus': 0.5, 'num_cpus': 2}, # remote
    route_prefix="/",
).bind()
