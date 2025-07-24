from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    spreadsheet_id={
        'type': ARG_TYPE.STR,
        'description': 'The unique ID of the Google Sheet.'
    },
    sheet_name={
        'type': ARG_TYPE.STR,
        'description': 'The name of the sheet within the Google Sheet.'
    }
)

connection_args_example = OrderedDict(
    spreadsheet_id='12wgS-1KJ9ymUM-6VYzQ0nJYGitONxay7cMKLnEE2_d0',
    sheet_name='iris'
)
