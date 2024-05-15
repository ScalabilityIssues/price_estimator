import asyncio
import hydra, os
from datetime import datetime
import json
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from omegaconf import DictConfig
from minio_client import MinioClient
from utils_scraper import (
    generate_date_range,
    generate_permutations,
    save_info,
    scrape,
)


# Produce a file and upload it to MinIO.
# If the upload fails write log in a file to allow the resuming of the process later
# Run only in force_scraping=True
async def main(cfg: DictConfig):
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME_TRAINING = os.getenv("MINIO_BUCKET_NAME_TRAINING")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

    client = MinioClient(
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )

    data_dir = os.listdir(cfg.get("output_data_dir"))
    if (
        len(data_dir) != 0
        and len([i for i, f in enumerate(data_dir) if ".csv" in f]) != 0
        and not cfg.get("force_scraping")
    ):
        latest_file = max(
            [
                cfg.get("output_data_dir") + f
                for f in data_dir
                if not f.endswith(".gitkeep")
            ],
            key=os.path.getctime,
        )
        if not client.exists_file(MINIO_BUCKET_NAME_TRAINING, latest_file):
            print("New files found, uploading to MinIO bucket...")
            client.upload_file(
                bucket_name=MINIO_BUCKET_NAME_TRAINING,
                source_dir=data_dir,
                file_name=latest_file,
                content_type="application/csv",
            )
            print(
                f"Files uploaded successfully to MinIO bucket {MINIO_BUCKET_NAME_TRAINING} from {data_dir}"
            )
    elif not client.is_empty(MINIO_BUCKET_NAME_TRAINING) and not cfg.get(
        "force_scraping"
    ):
        print("No scraping data found, downloading from MinIO bucket...")
        client.download_file(
            dest_dir=data_dir,
            bucket_name=MINIO_BUCKET_NAME_TRAINING,
            latest=True,
        )
        print(f"Files downloaded successfully to {data_dir}")
    else:
        print(
            "No data found in MinIO bucket or `force_scraping=True`, starting scraping..."
        )

        # Get the configuration parameters
        start_date = cfg.get("start_date")
        end_date = cfg.get("end_date")
        date_format = cfg.get("date_format")

        try:
            datetime.strptime(start_date, date_format)
            datetime.strptime(end_date, date_format)
        except ValueError:
            print(f"Invalid date format. Please use the format {date_format}")
            return

        # Generate all the date range and the possible flight permutations
        dates = generate_date_range(start_date, end_date)
        locations = list(str.split(cfg.get("locations"), ","))
        permutations = generate_permutations(dates, locations)

        iata_codes_mapping = {}
        num_workers = cfg.get("num_workers")
        headless = cfg.get("headless")
        with open(cfg.get("available_airports")) as f:
            iata_codes_mapping = json.load(f)

        # Scrape the data
        results = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            print("Scraping data in chunks...")
            print(
                "N. of data to retrieve:",
                len(permutations),
                "; ",
                "N. of workers:",
                num_workers,
                "; ",
                "Total chunks:",
                int((len(permutations) + num_workers - 1) / num_workers),
            )

            for i in range(0, int((len(permutations) + num_workers - 1) / num_workers)):
                print(f"CHUNK {i}")
                results += await asyncio.gather(
                    *[
                        scrape(
                            task_id=i,
                            browser=browser,
                            date=date,
                            source=source,
                            destination=destination,
                            iata_codes_mapping=iata_codes_mapping,
                        )
                        for i, (date, source, destination) in enumerate(
                            permutations[i * num_workers : (i + 1) * num_workers]
                        )
                    ]
                )
            await browser.close()

        save_info(data_dir, results)
        print(f"Data saved in {data_dir}")

        client.upload_file(
            bucket_name=MINIO_BUCKET_NAME_TRAINING,
            source_dir=data_dir,
            latest=True,
            content_type="application/csv",
        )
        print(
            f"Files uploaded successfully to MinIO bucket {MINIO_BUCKET_NAME_TRAINING} from {data_dir}"
        )


if __name__ == "__main__":

    @hydra.main(
        version_base="1.3", config_path="../configs/scraper", config_name="config"
    )
    def helper(cfg: DictConfig):
        load_dotenv()
        asyncio.run(main(cfg))

    helper()
