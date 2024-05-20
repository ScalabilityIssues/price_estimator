from time import ctime
from dotenv import load_dotenv
import os
from minio import Minio


def main():
    load_dotenv()
    MINIO_ENDPOINT = "localhost:9000"
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME_TRAINING")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
        raise Exception(f"Bucket {MINIO_BUCKET_NAME} does not exist")

    minio_client.fput_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name="test_object.csv",
        file_path="tests/test_file.csv",
        content_type="application/csv",
        metadata={"creation-date": ctime(os.path.getctime("tests/test_file.csv"))},
    )
    print("Object uploaded successfully")


if __name__ == "__main__":
    main()
