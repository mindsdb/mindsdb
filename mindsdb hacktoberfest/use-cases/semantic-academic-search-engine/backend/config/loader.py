"""YAML configuration loader."""
import os
from pathlib import Path
from typing import Any, Dict
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration singleton class."""
    
    _instance = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance
    
    def load(self, config_path: str | Path = None) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not config_path:
            # Default to config/config.yaml in project root
            config_path = Path(__file__).parent / "config.yaml"
        
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)
        
        # Override with environment variables if present
        self._override_from_env()
        
        return self._config
    
    def _override_from_env(self):
        """Override configuration values from environment variables."""

        if os.getenv("MINDSDB_URL"):
            self._config.setdefault("mindsdb", {})["url"] = os.getenv("MINDSDB_URL")
        
        # App configuration from environment
        if os.getenv("APP_PORT"):
            self._config.setdefault("app", {})["port"] = int(os.getenv("APP_PORT"))
        
        if os.getenv("APP_HOST"):
            self._config.setdefault("app", {})["host"] = os.getenv("APP_HOST")
        
        # OpenAI API key from environment
        if os.getenv("OPENAI_API_KEY"):
            self._config.setdefault("knowledge_base", {})
            self._config["knowledge_base"].setdefault("embedding_model", {})["api_key"] = os.getenv("OPENAI_API_KEY")
            self._config["knowledge_base"].setdefault("reranking_model", {})["api_key"] = os.getenv("OPENAI_API_KEY")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot notation key."""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    @property
    def all(self) -> Dict[str, Any]:
        """Get all configuration."""
        return self._config


# Singleton instance
_config_instance = Config()


def load_config(config_path: str | Path = None) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    return _config_instance.load(config_path)


def get_config() -> Config:
    """Get configuration instance."""
    # Auto-load config if not already loaded
    if not _config_instance._config:
        _config_instance.load()
    return _config_instance
