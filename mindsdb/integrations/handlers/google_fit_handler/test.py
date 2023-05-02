from __future__ import print_function

import os.path
import pytz
import time
from datetime import date as datedate, datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/fitness.activity.read']

def get_aggregate(fit_service, startTimeMillis, endTimeMillis, dataSourceId):
    return fit_service.users().dataset().aggregate(userId="me", body={
        "aggregateBy": [{
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": dataSourceId
        }],
        "bucketByTime": {"durationMillis": 86400000},
        "startTimeMillis": startTimeMillis,
        "endTimeMillis": endTimeMillis
    }).execute()

def main():
    """Shows basic usage of the Docs API.
    Prints the title of a sample document.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'E:\Coding\open source\mindsdb\mindsdb\mindsdb\integrations\handlers\google_fit_handler\credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        epoch0 = datetime(1970, 1, 1, tzinfo=pytz.utc)
        yesterday_local = datetime.now(pytz.timezone('US/Pacific')) - timedelta(days=1)
        local_0_hour = pytz.timezone('US/Pacific').localize(datetime(2023, 4, 29))
        start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
        end_time_millis = int(round(time.time() * 1000))
        fit_service = build('fitness', 'v1', credentials=creds)
        steps = {}
        steps_data = get_aggregate(fit_service, start_time_millis, end_time_millis, "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps")
        print(steps_data)
    except HttpError as err:
        print(err)

if __name__ == '__main__':
    main()