# file_uploader.py MinIO Python SDK example
from minio import Minio
from minio.error import S3Error
import os
from minio_client import MinioClient


def main():

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    DATA_DIR = os.getenv("DATA_DIR", "/data/scraped")

    # MINIO_ENDPOINT = "localhost:9000"
    # MINIO_BUCKET_NAME = "test-bucket"
    # MINIO_ACCESS_KEY = "root"
    # MINIO_SECRET_KEY = "root1234"
    # DATA_DIR = "data/scraped/"

    print(f"Uploading files from {DATA_DIR} to {MINIO_BUCKET_NAME} bucket")
    client = MinioClient(
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    client.upload_file(
        bucket_name=MINIO_BUCKET_NAME,
        source_dir=DATA_DIR,
        latest=True,
        content_type="application/csv",
    )
    print("Files uploaded successfully")


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("Error occurred in MinIO", exc)
