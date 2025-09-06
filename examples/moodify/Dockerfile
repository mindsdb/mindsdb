# Use official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Expose port
EXPOSE 8080

# Run Flask app
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]
