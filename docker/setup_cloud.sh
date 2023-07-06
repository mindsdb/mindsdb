#!/bin/bash

# Create mount directory for service
# Currently only setting up GCS will update for multiple cloud storages.

MINDSDB_CLOUD_DIR=$(jq '."cloud_storage_dir" //empty' /root/mindsdb_config.json)
echo "Cloud Mount dir configuration : $MINDSDB_CLOUD_DIR"
if [ -n "$MINDSDB_CLOUD_DIR" ] && [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -n "$GCS_BUCKET" ]
then
  echo "Mounting GCS Fuse."
  echo "Using application default target as : $GOOGLE_APPLICATION_CREDENTIALS\n"
  mkdir -p $MINDSDB_CLOUD_DIR;
  gcsfuse --debug_gcs --debug_fuse --debug_http $GCS_BUCKET $MINDSDB_CLOUD_DIR;
  echo "Mounting completed."
  echo "Cloud setup complete."
else
  echo "Cloud configuration not present using disk store."
fi;

exit 0