import asyncio
import json
import logging
import os
from datetime import datetime
from time import ctime

import hydra
from dotenv import load_dotenv
from minio import Minio
from omegaconf import DictConfig
from playwright.async_api import async_playwright

from progress import Progress
from utils_scrape import (generate_date_range, generate_permutations,
                          save_info, scrape)

log = logging.getLogger(__name__)

# Produce a file and upload it to MinIO.
# Run only if force_scraping is set to True in the config file.


async def main(cfg: DictConfig):

    if cfg.get("force_scraping"):
        load_dotenv()

        minio_client = Minio(
            endpoint=cfg.minio.endpoint,
            access_key=cfg.minio.access_key,
            secret_key=cfg.minio.secret_key,
            secure=cfg.minio.secure_connection,
        )
        if not minio_client.bucket_exists(cfg.minio.bucket_name_training):
            raise Exception(f"Bucket {cfg.minio.bucket_name_training} does not exist")

        # Get the configuration parameters
        start_date = cfg.get("start_date")
        end_date = cfg.get("end_date")
        date_format = cfg.get("date_format")
        output_data_dir = cfg.get("output_data_dir")

        try:
            datetime.strptime(start_date, date_format)
            datetime.strptime(end_date, date_format)
        except ValueError:
            log.error(f"Invalid date format. Please use the format {date_format}")
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
            log.info("Scraping data in chunks...")
            log.info(
                f"N. of data to retrieve:{len(permutations)}; N. of workers:{num_workers};" +
                f"Total chunks:{int((len(permutations) + num_workers - 1) / num_workers)}"
            )

            for i in range(0, int((len(permutations) + num_workers - 1) / num_workers)):
                log.info(f"CHUNK {i}")
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
                            permutations[i * num_workers: (i + 1) * num_workers]
                        )
                    ]
                )
            await browser.close()

        gen_filename = save_info(output_data_dir, results)
        log.info(f"Data saved in {output_data_dir}")

        # Upload the file to MinIO
        result = minio_client.fput_object(
            bucket_name=cfg.minio.bucket_name_training,
            object_name=gen_filename,
            file_path=output_data_dir + gen_filename,
            content_type="application/csv",
            progress=Progress(),
            metadata={
                "creation-date": ctime(os.path.getctime(output_data_dir + gen_filename))
            },
        )
        log.info(f"\nObject {result.object_name} uploaded to MinIO bucket")
    else:
        log.warning("Scraping not forced, skipping...")


if __name__ == "__main__":

    @hydra.main(
        version_base="1.3", config_path="../configs/scrape", config_name="config"
    )
    def helper(cfg: DictConfig):
        return asyncio.run(main(cfg))

    helper()
