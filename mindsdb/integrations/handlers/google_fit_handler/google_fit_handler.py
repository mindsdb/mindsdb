import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pytz
from datetime import datetime, timedelta
epoch0 = datetime(1970, 1, 1, tzinfo=pytz.utc)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/fitness.activity.read']

def retrieve_data(service, startTimeMillis, endTimeMillis, dataSourceId):
    return service.users().dataset().aggregate(userId="me", body={
        "aggregateBy": [{
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": dataSourceId
        }],
        "bucketByTime": {"durationMillis": 86400000},
        "startTimeMillis": startTimeMillis,
        "endTimeMillis": endTimeMillis
    }).execute()