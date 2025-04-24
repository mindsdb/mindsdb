import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Print all environment variables for debugging
print("Current environment variables:")
print("AWS_ACCESS_KEY_ID:", os.getenv('AWS_ACCESS_KEY_ID'))
print("AWS_SECRET_ACCESS_KEY:", os.getenv('AWS_SECRET_ACCESS_KEY'))
print("MINIO_ENDPOINT_URL:", os.getenv('MINIO_ENDPOINT_URL'))
print("MINIO_ACCESS_KEY:", os.getenv('MINIO_ACCESS_KEY'))
print("MINIO_SECRET_KEY:", os.getenv('MINIO_SECRET_KEY'))

# Verify .env file exists
env_path = os.path.join(os.path.dirname(__file__), '.env')
print("\n.env file path:", env_path)
print("Does .env file exist?", os.path.exists(env_path))

# If .env doesn't exist, create it with example values
if not os.path.exists(env_path):
    print("\nCreating .env file with example values...")
    with open(env_path, 'w') as f:
        f.write("""# AWS S3 Credentials for testing
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key

# MinIO Configuration
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
""")
    print("Created .env file. Please edit it with your actual credentials.")