#!/bin/bash

if [ -n "$MDB_CONFIG_CONTENT" ]; then
  echo "$MDB_CONFIG_CONTENT" > /root/mindsdb_config.json;
fi;

if [ -n "$MDB_AUTOUPDATE" ]; then
  VERSION=$(curl "https://public.api.mindsdb.com/installer/$MDB_RELTYPE/docker___started___None")
  echo "--- Updating to MindsDB $VERSION ---"
  pip install mindsdb=="$VERSION"
fi;

python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb
