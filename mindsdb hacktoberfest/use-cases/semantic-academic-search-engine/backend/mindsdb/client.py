"""MindsDB client wrapper for easy interaction."""
import logging
from typing import Any, Dict, List, Optional
import mindsdb_sdk

from config import get_config

logger = logging.getLogger(__name__)


class MindsDBClient:
    """MindsDB client wrapper."""
    
    def __init__(self, url: str = None, email: str = None, password: str = None):
        """Initialize MindsDB client.
        
        Args:
            url: MindsDB server URL
            email: MindsDB account email (for cloud)
            password: MindsDB account password (for cloud)
        """
        config = get_config()
        
        self.url = url or config.get("mindsdb.url")
        self.email = email or config.get("mindsdb.email")
        self.password = password or config.get("mindsdb.password")
        
        self._server = None
        self._connected = False
    
    def connect(self) -> bool:
        """Connect to MindsDB server.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.email and self.password:
                # Cloud connection
                logger.info(f"Connecting to MindsDB Cloud: {self.url}")
                self._server = mindsdb_sdk.connect(
                    login=self.email,
                    password=self.password
                )
            else:
                # Local connection
                logger.info(f"Connecting to MindsDB Local: {self.url}")
                self._server = mindsdb_sdk.connect(self.url)
            
            self._connected = True
            logger.info("Successfully connected to MindsDB")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MindsDB: {e}")
            self._connected = False
            return False
    
    def disconnect(self):
        """Disconnect from MindsDB server."""
        self._server = None
        self._connected = False
        logger.info("Disconnected from MindsDB")
    
    def is_connected(self) -> bool:
        """Check if connected to MindsDB."""
        return self._connected
    
    def query(self, sql: str) -> Any:
        """Execute SQL query on MindsDB.
        
        Args:
            sql: SQL query string
            
        Returns:
            Query result
        """
        if not self._connected:
            raise ConnectionError("Not connected to MindsDB. Call connect() first.")
        
        try:
            logger.debug(f"Executing query: {sql}")
            result = self._server.query(sql)
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def list_databases(self) -> List[str]:
        """List all databases.
        
        Returns:
            List of database names
        """
        if not self._connected:
            raise ConnectionError("Not connected to MindsDB. Call connect() first.")
        
        try:
            databases = self._server.list_databases()
            return [db.name for db in databases]
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            raise
    
    @property
    def server(self):
        """Get raw MindsDB server instance."""
        return self._server


# Global client instance
_client_instance: Optional[MindsDBClient] = None


def get_mindsdb_client() -> MindsDBClient:
    """Get or create MindsDB client singleton.
    
    Returns:
        MindsDBClient instance
    """
    global _client_instance
    
    if _client_instance is None:
        _client_instance = MindsDBClient()
    
    return _client_instance
