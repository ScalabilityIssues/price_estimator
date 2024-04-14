# file_uploader.py MinIO Python SDK example
from minio import Minio
from minio.error import S3Error
import os, rootutils
from time import ctime

rootutils.setup_root(__file__, indicator=".project-root", pythonpath=True)
from src.progress import Progress


def main():

    # MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    # MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    # MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    # MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    # DATA_DIR = os.getenv("DATA_DIR", "/data")

    MINIO_ENDPOINT = "localhost:9000"
    MINIO_BUCKET_NAME = "test-bucket"
    MINIO_ACCESS_KEY = "root"
    MINIO_SECRET_KEY = "root1234"
    DATA_DIR = "data/scraped/"

    secure = True
    if "localhost" in MINIO_ENDPOINT or "127.0.0.1" in MINIO_ENDPOINT:
        secure = False
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )

    # The file to upload
    file_name = ""
    source_file = ""
    if os.path.exists(DATA_DIR):
        all_files = [DATA_DIR + f for f in os.listdir(DATA_DIR)]
        latest = max(all_files, key=os.path.getctime)
        source_file = latest
        file_name = os.path.basename(latest)
    else:
        print("Data directory not found")
        return

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(MINIO_BUCKET_NAME)
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
        print("Created bucket", MINIO_BUCKET_NAME)
    else:
        print("Bucket", MINIO_BUCKET_NAME, "already exists")

    # Upload the file, renaming it in the process
    result = client.fput_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=file_name,
        file_path=source_file,
        content_type="application/csv",
        progress=Progress(),
        metadata={"creation-date": ctime(os.path.getctime(source_file))},
    )
    print(f"\nCreated {result.object_name} object")


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)
