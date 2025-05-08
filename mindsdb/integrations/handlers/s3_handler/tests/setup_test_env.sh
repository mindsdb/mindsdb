#!/bin/bash

# Start MinIO server
echo "Starting MinIO server..."
docker run -d -p 9000:9000 -p 9001:9001 --name minio-test minio/minio server /data --console-address ":9001"

# Wait for MinIO to start
echo "Waiting for MinIO to start..."
sleep 10

# Create MinIO bucket using mc (MinIO Client)
echo "Setting up MinIO bucket..."
docker run --rm --entrypoint=/bin/sh minio/mc -c "
  mc alias set myminio http://host.docker.internal:9000 minioadmin minioadmin
  mc mb myminio/test-bucket
  mc policy set public myminio/test-bucket
"

echo "MinIO setup complete!"
echo "MinIO Console: http://localhost:9001"
echo "MinIO Endpoint: http://localhost:9000"
echo "MinIO Credentials: minioadmin/minioadmin"
echo "MinIO Bucket: test-bucket"

echo ""
echo "For AWS S3 tests, you need to:"
echo "1. Create an AWS account if you don't have one"
echo "2. Create an IAM user with S3 access"
echo "3. Create access keys for the IAM user"
echo "4. Create a bucket named 'mindsdb-bucket'"
echo "5. Update the test credentials in test_s3_endpoints.py" 