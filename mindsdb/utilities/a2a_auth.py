import secrets
import time
import requests
from typing import Dict, Any, Optional

from mindsdb.utilities.config import Config
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# In-memory storage for A2A tokens (in production, this should be a database)
_a2a_tokens: Dict[str, Dict[str, Any]] = {}


def generate_auth_token() -> str:
    """Generate a secure authentication token"""
    return secrets.token_urlsafe(32)


def store_auth_token(token: str, user_id: str, description: str = "") -> None:
    """Store an authentication token with metadata"""
    _a2a_tokens[token] = {
        'created_at': time.time(),
        'description': description,
        'user_id': user_id
    }


def is_auth_token_valid(token: str) -> bool:
    """Check if an authentication token is valid and not expired"""
    if token not in _a2a_tokens:
        return False
    
    token_data = _a2a_tokens[token]
    # Check if token is expired (24 hours)
    if time.time() - token_data['created_at'] > 86400:
        del _a2a_tokens[token]
        return False
    
    return True


def validate_auth_token_remote(token: str, http_api_url: str = None) -> bool:
    """Validate authentication token by calling the HTTP API (for separate servers)"""
    if http_api_url is None:
        # Get from config
        config = Config()
        http_host = config.get('api', {}).get('http', {}).get('host', 'localhost')
        http_port = config.get('api', {}).get('http', {}).get('port', 47334)
        http_api_url = f"http://{http_host}:{http_port}/api"
    
    try:
        response = requests.post(
            f"{http_api_url}/auth_tokens/token/validate",
            json={"token": token},
            timeout=5
        )
        return response.status_code == 200
    except Exception as e:
        logger.warning(f"Failed to validate A2A token remotely: {e}")
        return False


def get_auth_token_data(token: str) -> Optional[Dict[str, Any]]:
    """Get authentication token data if valid"""
    if is_auth_token_valid(token):
        return _a2a_tokens[token]
    return None


def revoke_auth_token(token: str) -> bool:
    """Revoke an authentication token"""
    if token in _a2a_tokens:
        del _a2a_tokens[token]
        return True
    return False


def get_auth_required_status() -> bool:
    """Check if authentication is required based on HTTP auth configuration"""
    config = Config()
    return config['auth']['http_auth_enabled'] is True


def cleanup_expired_tokens() -> int:
    """Clean up expired tokens and return count of cleaned tokens"""
    current_time = time.time()
    expired_tokens = [
        token for token, data in _a2a_tokens.items()
        if current_time - data['created_at'] > 86400
    ]
    for token in expired_tokens:
        del _a2a_tokens[token]
    return len(expired_tokens)


def get_all_auth_tokens() -> Dict[str, Dict[str, Any]]:
    """Get all authentication tokens (for admin purposes)"""
    # Clean up expired tokens first
    cleanup_expired_tokens()
    return _a2a_tokens.copy()
