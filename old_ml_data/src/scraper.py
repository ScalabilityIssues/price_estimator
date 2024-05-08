import asyncio
from typing import Dict
import hydra, os
from datetime import datetime
import json

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser
from omegaconf import DictConfig
from minio_client import MinioClient
from utils_scraper import (
    generate_date_range,
    generate_permutations,
    get_direct_flights_mask,
    get_flight_price,
    get_flight_times,
    get_timezone,
    save_info,
)


async def scrape(
    task_id: int,
    browser: Browser,
    date: str,
    source: str,
    destination: str,
    iata_codes_mapping: Dict[str, str],
    locale: str = "en-US",
    prefix_url: str = "https://www.kayak.com/flights/",
    timeout: int = 60000,
):
    """
    Scrapes flight data from Kayak website.

    Args:
        task_id (int): The ID of the task.
        browser (Browser): The browser instance used for scraping.
        date (str): The date of the flight.
        source (str): The source airport code.
        destination (str): The destination airport code.
        iata_codes_mapping (Dict[str, str]): A dictionary mapping airport codes to timezones.
        locale (str, optional): The locale to use for the browser. Defaults to "en-US".
        prefix_url (str, optional): The prefix URL for the flight search. Defaults to "https://www.kayak.com/flights/".
        timeout (int, optional): The timeout for page navigation. Defaults to 60000 (60 seconds).

    Returns:
        List: A list containing the scraped flight data, including date, source, destination, start times, end times, prices, and currencies.
    """
    # Generate the url, stops=~0 means approx direct flights only
    url = f"{prefix_url}{source}-{destination}/{date}?stops=~0&sort=bestflight_a"

    # Initialize the browser context
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    context = await browser.new_context(
        user_agent=user_agent,
        viewport={"width": 1920, "height": 1080},
        screen={"width": 1920, "height": 1080},
        locale=locale,
    )
    page = await context.new_page()

    try:
        # Start true scraping
        await page.goto(url, timeout=timeout)
        tz_start = get_timezone(iata_codes_mapping[source])
        tz_end = get_timezone(iata_codes_mapping[destination])

        # Accept cookies
        try:
            await page.locator(
                "xpath=//div[@class='RxNS-button-content' ]", has_text="Accept all"
            ).click()
        except Exception as e:
            print(f"TASK {task_id} - no cookies to accept")

        # Click "show more" button to get more flights
        try:
            for _ in range(2):
                await page.locator(
                    "xpath=//*[contains(@class, 'show-more-button')]"
                ).click()
                await asyncio.sleep(5)
        except Exception as e:
            print(f"TASK {task_id} - no more flights to show")

        page_source = await page.content()
        soup = BeautifulSoup(page_source, "html.parser")

        direct_flights_mask = get_direct_flights_mask(soup)
        start_times, end_times = get_flight_times(
            soup, direct_flights_mask, tz_start, tz_end
        )
        prices, currencies = get_flight_price(soup, direct_flights_mask)
        print(
            f"TASK {task_id} - prices: {len(prices)} start_times: {len(start_times)} end_times: {len(end_times)} currencies: {len(currencies)}\n"
        )
        return [date, source, destination, start_times, end_times, prices, currencies]
    except Exception as e:
        print(f"Error scraping data from {url}: {e}")
    finally:
        await context.close()
    return []


async def main(cfg: DictConfig):
    dir = os.listdir(cfg.get("output_data_dir"))
    latest_file = max([f for f in dir], key=os.path.getctime)

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    DATA_DIR = os.getenv("DATA_DIR", "/data/scraped")

    client = MinioClient(
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )

    if (
        not cfg.get("run_once")
        or len(dir) == 0
        or client.check_file_exists(MINIO_BUCKET_NAME, latest_file)
    ):

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

        output_data_dir = cfg.get("output_data_dir")
        data_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".csv"
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

        save_info(output_data_dir, data_filename, results)
        print(f"Data saved in {output_data_dir + data_filename}")

        client.upload_file(
            bucket_name=MINIO_BUCKET_NAME,
            source_dir=DATA_DIR,
            latest=True,
            content_type="application/csv",
        )
        print(
            f"Files uploaded successfully to MinIO bucket {MINIO_BUCKET_NAME} from {DATA_DIR}"
        )

    else:
        print("Data already exists, set `run_once` to false to scrape again.")


if __name__ == "__main__":

    @hydra.main(
        version_base="1.3", config_path="../scraper_configs", config_name="config"
    )
    def helper(cfg: DictConfig):
        asyncio.run(main(cfg))

    helper()