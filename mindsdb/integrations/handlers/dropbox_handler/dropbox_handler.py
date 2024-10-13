from mindsdb.integrations.libs.api_handler import APIHandler

class DropboxHandler(APIHandler):

    """
    This handler handles connection and execution of the SQL statements on files from DropBox.
    """

    name = 'dropbox'
    supported_file_formats = ['csv', 'tsv', 'json']
    
    def __init__(self, name: str):
        super().__init__(name)
