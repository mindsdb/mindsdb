from pathlib import Path
from typing import Optional

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.api_resource_generator import APIResourceGenerator
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class JSONPlaceholderHandler(APIHandler):
    """
    This handler handles connection and execution SQL statements on the JSONPlaceholder API.
    """

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the AWS (S3) account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        current_dir = Path(__file__).parent
        openapi_file_path = current_dir / 'openapi.json'
        api_resource_generator = APIResourceGenerator(
            base_url='https://jsonplaceholder.typicode.com',
            openapi_file=openapi_file_path,
        )
        for resource, resource_class in api_resource_generator.create_resource_classes():
            self._register_table(resource, resource_class(self))

    def check_connection(self) -> StatusResponse:
        """
        Checks the connection to the JSONPlaceholder API.
        """
        response = StatusResponse(True)
        return response

