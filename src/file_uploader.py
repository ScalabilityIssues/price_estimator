# file_uploader.py MinIO Python SDK example
from minio import Minio
import hydra
from minio.error import S3Error
from omegaconf import DictConfig


@hydra.main(config_path="./configs", config_name="config")
def main(cfg: DictConfig):
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio("localhost:9000", #or :9001
        access_key="root",
        secret_key="root123",
    )

    # The file to upload, change this path if needed
    source_file = cfg.get("output_data_dir")+cfg.get("data_filename")

    # The destination bucket and filename on the MinIO server
    bucket_name = "ml-data-bucket"
    destination_file = cfg.get("data_filename")

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    client.fput_object(
        bucket_name, destination_file, source_file,
    )
    print(
        source_file, "successfully uploaded as object",
        destination_file, "to bucket", bucket_name,
    )


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)