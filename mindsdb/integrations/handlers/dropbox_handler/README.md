# DropBox Handler

This documentation describes the integration of MindsDB with [Dropbox](https://www.dropbox.com/official-teams-page?_tk=paid_sem_goog_biz_b&_camp=1033325405&_kw=dropbox|e&_ad=708022104237||c&gad_source=1&gclid=EAIaIQobChMI3qGNp4WPiQMVMpeDBx0X3CdpEAAYASAAEgIb9PD_BwE), a storage service.

## Connection

Establish a connection to your Dropbox account from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE dropbox_datasource
WITH
    engine = 'dropbox',
    parameters = {
      "access_token": "ai.L-wqp3eP6r4cSWVklkKAdTNZ3VAuQjWuZMvIs1BzKvZNVW07rKbVNi5HbxvLc9q9D6qSfsf5VTsqYsNPGUkqSJBlpkr88gNboUNuhITmJG9mVw-Olniu4MO3BWVbEIphVxXxxxCd677Y"
    };
```

Required connection parameters include the following:

- `access_token`: The Dropbox access token that enables connection to your Dropbox app.
