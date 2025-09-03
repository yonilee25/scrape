from minio import Minio
from minio.error import S3Error
from libs.common.config import Settings
import io

settings = Settings()
client = Minio(
    settings.MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=settings.MINIO_ENDPOINT.startswith("https://")
)

def ensure_bucket():
    found = client.bucket_exists(settings.MINIO_BUCKET)
    if not found:
        client.make_bucket(settings.MINIO_BUCKET)

def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream"):
    ensure_bucket()
    client.put_object(settings.MINIO_BUCKET, key, io.BytesIO(data), length=len(data), content_type=content_type)
    return f"s3://{settings.MINIO_BUCKET}/{key}"

def put_file(key: str, file_path: str, content_type: str = "application/octet-stream"):
    ensure_bucket()
    client.fput_object(settings.MINIO_BUCKET, key, file_path, content_type=content_type)
    return f"s3://{settings.MINIO_BUCKET}/{key}"
