#!/bin/bash

if [[ -n "$MDB_CONFIG_CONTENT" ]]; then
  echo "$MDB_CONFIG_CONTENT" > /root/mindsdb_config.json;
fi;

if [[ -n "$MDB_AUTOUPDATE" ]]; then
  URL="https://public.api.mindsdb.com/installer/$MDB_RELTYPE/docker___started___None"
  VERSION=$(python -c "import urllib.request as r; print(r.urlopen('$URL').read().decode())")
  echo "--- Updating to MindsDB $VERSION ---"
  pip install mindsdb=="$VERSION"
fi;

if [[ "$1" == "start" ]]; then
  python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb
fi;
