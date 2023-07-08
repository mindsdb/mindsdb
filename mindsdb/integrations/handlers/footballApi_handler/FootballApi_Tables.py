from mindsdb.integrations.libs.api_handler import APITable


class FootballApiTable(APITable):
    def __init__(self, handler):
        super().__init__(handler)