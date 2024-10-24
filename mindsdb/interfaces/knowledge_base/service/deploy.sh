REMOTE_DEPLOYMENT_DIR=/home/azureuser/ingestion_pipeline
LOCAL_DIR=.
REMOTE_SERVER=azureuser@104.46.202.127
# rsync command to send the files to the remote server
rsync -avz $LOCAL_DIR $REMOTE_SERVER:$REMOTE_DEPLOYMENT_DIR --exclude=volumes

