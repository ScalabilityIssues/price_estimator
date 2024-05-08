import time
from minio import Minio
import os
from time import ctime
from progress import Progress


class MinioClient:
    """
    A class that represents a Minio client for uploading and downloading files to/from a Minio server.

    Args:
        endpoint (str): The Minio server endpoint URL.
        access_key (str): The access key for the Minio server.
        secret_key (str): The secret key for the Minio server.
        secure (bool, optional): Whether to use secure (HTTPS) connection. Defaults to True.
    """

    def __init__(self, endpoint, access_key, secret_key, secure=True):
        self.endpoint = endpoint
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def upload_file(
        self,
        bucket_name,
        source_dir,
        file_name="",
        latest=True,
        content_type="application/csv",
    ):
        """
        Uploads a file to the specified Minio bucket.

        Args:
            bucket_name (str): The name of the Minio bucket.
            source_dir (str): The directory path where the file is located.
            file_name (str, optional): The name of the file to upload. If not provided, the latest file in the directory will be uploaded. Defaults to "".
            latest (bool, optional): Whether to upload the latest file in the directory. Defaults to True.
            content_type (str, optional): The content type of the file. Defaults to "application/csv".
        """
        # The file to upload
        source_path = ""
        if os.path.exists(source_dir):
            if latest:
                all_files = [source_dir + f for f in os.listdir(source_dir)]
                latest_file = max(all_files, key=os.path.getctime)
                source_path = latest  # data_dir + latest_filename
                file_name = os.path.basename(latest_file)
            else:
                source_path = source_dir + file_name
        else:
            print("Data directory not found")
            return

        # Make the bucket if it doesn't exist.
        found = self.client.bucket_exists(bucket_name)
        if not found:
            self.client.make_bucket(bucket_name)
            print("Created bucket", bucket_name)
        else:
            print("Bucket", bucket_name, "already exists")

        # Upload the file, renaming it in the process
        result = self.client.fput_object(
            bucket_name=bucket_name,
            object_name=file_name,
            file_path=source_path,
            content_type=content_type,
            progress=Progress(),
            metadata={"creation-date": ctime(os.path.getctime(source_path))},
        )
        print(f"\nCreated {result.object_name} object")

    def download_file(self, bucket_name, dest_dir, file_name="", latest=True):
        """
        Downloads a file from the specified Minio bucket.

        Args:
            bucket_name (str): The name of the Minio bucket.
            dest_dir (str): The directory path where the file will be downloaded.
            file_name (str, optional): The name of the file to download. If not provided, the latest file in the bucket will be downloaded. Defaults to "".
            latest (bool, optional): Whether to download the latest file in the bucket. Defaults to True.

        Returns:
            file: The downloaded file object.
        """
        found = self.client.bucket_exists(bucket_name)
        if not found:
            print("Bucket", bucket_name, "not found")
            return

        objects = self.client.list_objects(bucket_name, include_user_meta=True)
        all_files = [
            (
                o.object_name,
                time.mktime(
                    time.strptime(
                        o.metadata["X-Amz-Meta-Creation-Date"], "%a %b %d %H:%M:%S %Y"
                    )
                ),
            )
            for o in objects
        ]
        if len(all_files) == 0:
            print("No files found in the bucket")
            return

        if latest:
            file_name = max(all_files, key=lambda x: x[1])[0]

        file_path = dest_dir + file_name

        file = self.client.fget_object(
            bucket_name=bucket_name,
            object_name=file_name,
            file_path=file_path,
        )
        return file

    def check_empty(self, bucket_name):
        """
        Checks if a bucket is empty.

        Args:
            bucket_name (str): The name of the Minio bucket.

        Returns:
            bool: True if the bucket is empty, False otherwise.
        """
        objects = self.client.list_objects(bucket_name)
        return objects == None