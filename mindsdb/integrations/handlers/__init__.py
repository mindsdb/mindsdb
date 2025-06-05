import os
from mindsdb.integrations.libs.const import HANDLER_TYPE

def get_enabled_handlers():
    """Get list of enabled handlers based on environment variable"""
    if os.environ.get('MINDSDB_ENABLED_HANDLERS'):
        return os.environ.get('MINDSDB_ENABLED_HANDLERS').split(',')
    return None  # Load all available handlers

def load_handler_modules():
    """Load only enabled handler modules"""
    enabled_handlers = get_enabled_handlers()
    
    if enabled_handlers:
        # Load only specified handlers
        handlers = {}
        for handler_name in enabled_handlers:
            handler_name = handler_name.strip()
            try:
                module = __import__(f'mindsdb.integrations.handlers.{handler_name}_handler', 
                                   fromlist=[handler_name])
                handlers[handler_name] = module
            except ImportError as e:
                print(f"Warning: Could not load handler {handler_name}: {e}")
        return handlers
    else:
        # Load all handlers (default behavior)
        return load_all_handlers()

def load_all_handlers():
    """Load all available handlers (default behavior)"""
    import importlib
    from pathlib import Path
    
    handlers = {}
    handlers_path = Path(__file__).parent
    
    for handler_dir in handlers_path.iterdir():
        if handler_dir.is_dir() and not handler_dir.name.startswith('__'):
            handler_name = handler_dir.name.replace('_handler', '')
            try:
                module = __import__(f'mindsdb.integrations.handlers.{handler_dir.name}', 
                                   fromlist=[handler_name])
                handlers[handler_name] = module
            except ImportError as e:
                print(f"Warning: Could not load handler {handler_name}: {e}")
    
    return handlers

# Initialize handlers based on environment
_handlers = load_handler_modules()