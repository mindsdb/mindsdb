FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy the entire application code
COPY ./src ./src
COPY ./notebooks ./notebooks
COPY ./scripts ./scripts
COPY ./data ./data
COPY ./tests ./tests
COPY ./docs ./docs
COPY .env.example .env.example

# Create a startup script to run both FastAPI and Streamlit
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Waiting for MindsDB to be ready..."\n\
until curl -f http://${MINDSDB_HOST:-mindsdb}:${MINDSDB_PORT:-47334} > /dev/null 2>&1; do\n\
  echo "MindsDB is unavailable - sleeping"\n\
  sleep 2\n\
done\n\
\n\
echo "MindsDB is ready! Starting application..."\n\
echo "Starting FastAPI on port 8000..."\n\
uvicorn src.app:app --host 0.0.0.0 --port 8000 &\n\
\n\
echo "Starting Streamlit on port 8501..."\n\
streamlit run src/ui/streamlit_app.py --server.port=8501 --server.address=0.0.0.0\n\
' > /app/start.sh && chmod +x /app/start.sh

# Expose ports for both FastAPI and Streamlit
EXPOSE 8000 8501

# Command to run both services
CMD ["/app/start.sh"]