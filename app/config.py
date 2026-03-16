import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "nutrition_dw")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "de_pass")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "user_profiles")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "nutrition-consumer-group")

# MinIO
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "nutrition-platform")

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
