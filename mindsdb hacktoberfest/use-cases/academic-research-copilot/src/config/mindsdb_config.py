# Configuration settings for connecting to MindsDB

from pydantic_settings import BaseSettings
from functools import lru_cache
import os


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # MindsDB Configuration
    mindsdb_host: str = "localhost"
    mindsdb_port: int = 47334
    mindsdb_user: str = "mindsdb"
    mindsdb_password: str = ""
    mindsdb_database: str = "mindsdb"
    
    # DuckDB Configuration
    duckdb_path: str = "data/academic_papers.duckdb"
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = True
    
    # Streamlit Configuration
    streamlit_host: str = "0.0.0.0"
    streamlit_port: int = 8501
    
    # Knowledge Base Configuration
    kb_name: str = "academic_kb"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Legacy constants for backward compatibility
MINDSDB_HOST = os.getenv("MINDSDB_HOST", "localhost")
MINDSDB_PORT = int(os.getenv("MINDSDB_PORT", "47334"))
MINDSDB_USER = os.getenv("MINDSDB_USER", "mindsdb")
MINDSDB_PASSWORD = os.getenv("MINDSDB_PASSWORD", "")
MINDSDB_DATABASE = os.getenv("MINDSDB_DATABASE", "mindsdb")