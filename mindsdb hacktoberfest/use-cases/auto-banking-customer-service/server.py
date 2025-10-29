#!/usr/bin/env python3
"""Legacy entry point preserved for backward compatibility."""

from app import app

if __name__ == "__main__":
    import uvicorn
    print("Starting Banking Customer Service API Server...")
    print("API Documentation: http://localhost:8000/docs")
    print("Health Check: http://localhost:8000/health")
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
