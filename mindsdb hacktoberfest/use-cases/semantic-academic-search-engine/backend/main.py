"""Main FastAPI application."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import load_config, get_config
from mindsdb import get_mindsdb_client
from api import router
from startup import run_startup_operations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting up application...")
    
    # Load configuration
    try:
        load_config()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise
    
    # Connect to MindsDB
    client = get_mindsdb_client()
    try:
        client.connect()
        logger.info("Connected to MindsDB successfully")
    except Exception as e:
        logger.warning(f"Failed to connect to MindsDB on startup: {e}")
        logger.warning("Application will start but MindsDB features may be unavailable")
    
    # Run startup operations (database setup, knowledge base, etc.)
    try:
        startup_results = run_startup_operations(skip_on_error=True)
        
        if startup_results.get("enabled"):
            summary = startup_results.get("summary", {})
            logger.info(f"Startup operations completed: {summary.get('successful')}/{summary.get('total')} successful")
            
            if not startup_results.get("all_successful"):
                logger.warning("Some startup operations failed or were skipped")
        else:
            logger.info("Startup operations disabled in configuration")
            
    except Exception as e:
        logger.error(f"Error running startup operations: {e}")
        logger.warning("Application will continue but some features may not be available")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    client.disconnect()


# Create FastAPI application
config = get_config()
app = FastAPI(
    title=config.get("api.title", "MindsDB REST API"),
    description=config.get("api.description", "REST API for interacting with MindsDB"),
    version=config.get("app.version", "1.0.0"),
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
api_prefix = config.get("api.prefix", "/api/v1")
app.include_router(router, prefix=api_prefix, tags=["MindsDB"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "MindsDB REST API",
        "version": config.get("app.version", "1.0.0"),
        "docs": "/docs",
        "api_prefix": api_prefix
    }


@app.get("/startup-status")
async def startup_status():
    """Get startup operations status."""
    # This could be enhanced to store and retrieve actual startup results
    return {
        "message": "Startup operations run during application initialization",
        "note": "Check application logs for detailed startup operation results"
    }


if __name__ == "__main__":
    import uvicorn
    
    host = config.get("app.host", "0.0.0.0")
    port = config.get("app.port", 8000)
    debug = config.get("app.debug", False)
    
    logger.info(f"Starting server on {host}:{port}")
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=debug,
        log_level="info"
    )
