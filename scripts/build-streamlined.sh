#!/bin/bash
set -e

# Build script for MindsDB Streamlined Docker image

echo "=========================================="
echo "Building MindsDB Streamlined Docker Image"
echo "=========================================="

# Set image name and tag
IMAGE_NAME="mindsdb-streamlined"
TAG=${1:-"latest"}
FULL_IMAGE_NAME="${IMAGE_NAME}:${TAG}"

echo "Image: ${FULL_IMAGE_NAME}"
echo "Build context: $(pwd)"
echo ""

# Build the Docker image
echo "Building Docker image..."
docker build \
    -f Dockerfile.streamlined \
    -t ${FULL_IMAGE_NAME} \
    --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
    --build-arg VCS_REF=$(git rev-parse --short HEAD) \
    .

echo ""
echo "Build completed successfully!"
echo "Image: ${FULL_IMAGE_NAME}"
echo ""

# Display image information
echo "Image details:"
docker images ${IMAGE_NAME}:${TAG}

echo ""
echo "To run the container:"
echo "docker run -d --name mindsdb-streamlined -p 47334:47334 ${FULL_IMAGE_NAME}"
echo ""
echo "To run with Docker Compose:"
echo "docker-compose -f docker-compose.streamlined.yml up -d"