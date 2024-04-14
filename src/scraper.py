from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo
import hydra, csv, os
from datetime import datetime, timedelta
from time import sleep
import chromedriver_autoinstaller
import itertools, json, pytz
import rootutils
from geopy.geocoders import Nominatim
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

rootutils.setup_root(__file__, indicator=".project-root", pythonpath=True)
from bs4 import BeautifulSoup
from omegaconf import DictConfig
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Scraper:
    def __init__(
        self,
        prefix_url: str = "https://www.kayak.com/flights/",
        headless=True,
    ):
        self.prefix_url = prefix_url
        # check if chrome driver is installed correctly and on path
        chromedriver_autoinstaller.install()

        self.options = webdriver.ChromeOptions()
        if headless:
            self.options.add_argument("--headless=new")
            self.options.add_argument("--window-size=1920x1080")
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            self.options.add_argument(f"--user-agent={user_agent}")
        self.driver = webdriver.Chrome(options=self.options)

    def __del__(self):
        self.driver.quit()

    def _get_website_info(
        self, url: str, tz_start: str, tz_end: str
    ) -> Tuple[List[str], List[str], List[float], List[str]]:

        print(f"Scraping data from {url}")
        self.driver.get(url)
        # print(self.driver.page_source)
        sleep(2)

        # Accept cookies
        try:
            button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//div[@class='RxNS-button-content' and text()='Accept all']",
                    )
                )
            )
            button.click()
        except Exception as e:
            print("No cookies to accept")

        # Click "show more" button to get more flights
        for _ in range(2):
            try:
                element = WebDriverWait(self.driver, 20).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//*[contains(@class, 'show-more-button')]")
                    )
                )
                element.click()
            except Exception as e:
                print("No more flights to show")
            sleep(5)

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        direct_flights_mask = Scraper.get_direct_flights_mask(soup)
        start_times, end_times = Scraper.get_flight_times(
            soup, direct_flights_mask, tz_start, tz_end
        )
        prices, currencies = Scraper.get_flight_price(soup, direct_flights_mask)
        print(
            f"Scraping summary - prices: {len(prices)} start_times: {len(start_times)} end_times: {len(end_times)} currencies: {len(currencies)}\n"
        )

        return start_times, end_times, prices, currencies

    def save_info(
        self,
        output_data_dir: str,
        data_filename: str,
        date: str,
        source: str,
        destination: str,
        start_times: List[str],
        end_times: List[str],
        prices: List[float],
        currencies: List[str],
    ):
        filename = data_filename
        file_exists = os.path.isfile(output_data_dir + filename)

        with open(output_data_dir + filename, "a", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            if not file_exists:
                writer.writerow(
                    [
                        "date",
                        "source",
                        "destination",
                        "start_time",
                        "end_time",
                        "price",
                        "currency",
                    ]
                )
            for row_data in zip(start_times, end_times, prices, currencies):
                row = [
                    date,
                    source,
                    destination,
                ] + list(row_data)
                writer.writerow(row)

    @staticmethod
    def _generate_permutations(
        dates: List[str], locations: List[str]
    ) -> List[Tuple[str, str, str]]:
        # Generate all permutations of locations
        location_permutations = list(itertools.permutations(locations, 2))
        # Generate all permutations of dates and location pairs
        permutations = [
            (date, loc1, loc2) for date in dates for loc1, loc2 in location_permutations
        ]
        return permutations

    def scrape(
        self,
        output_data_dir: str,
        data_filename: str,
        date_format: str,
        dates: List[str],
        locations: List[str],
        iata_codes_mapping: Dict[str, str],
    ):
        permuts = Scraper._generate_permutations(dates, locations)
        print("Scraping data...")
        for i, permut in enumerate(permuts):
            print("Iteration: ", i)
            date, source, destination = permut
            try:
                datetime.strptime(date, date_format)
            except ValueError:
                print(f"Date {date} is not in format {date_format}")
                continue

            # Close and open driver every 5 dates to avoid captcha
            if i != 0 and i % 5 == 0:
                self.driver.quit()
                self.driver = webdriver.Chrome(options=self.options)

            # Generate the url, stops=~0 means direct flights only
            url = f"{self.prefix_url}{source}-{destination}/{date}?stops=~0&sort=bestflight_a"

            tz_start = Scraper.get_timezone(iata_codes_mapping[source])
            tz_end = Scraper.get_timezone(iata_codes_mapping[destination])
            start_times, end_times, prices, currencies = self._get_website_info(
                url, tz_start, tz_end
            )
            self.save_info(
                output_data_dir,
                data_filename,
                date,
                source,
                destination,
                start_times,
                end_times,
                prices,
                currencies,
            )
            print("Saved data to csv file\n")

    @staticmethod
    def get_flight_times(
        soup, direct_flights_mask: List[int], tz_start: str, tz_end: str
    ) -> Tuple[List[str], List[str]]:
        times = soup("div", class_="vmXl vmXl-mod-variant-large")

        # Find all span tags and extract the text
        # Parse it as datetime in the format %I:%M %p (12-hour format)
        # Convert it to 24-hour format
        start_times = []
        end_times = []
        for mask, s in zip(direct_flights_mask, times):
            if mask:
                start_time = datetime.strptime(
                    str(s.find_all("span")[0].text), "%I:%M %p"
                )
                start_times.append(
                    datetime(
                        2000,
                        1,
                        1,
                        start_time.hour,
                        start_time.minute,
                        tzinfo=ZoneInfo(tz_start),
                    ).strftime("%H:%M%z")
                )

                end_time = datetime.strptime(
                    str(s.find_all("span")[2].text), "%I:%M %p"
                )
                end_times.append(
                    datetime(
                        2000,
                        1,
                        1,
                        end_time.hour,
                        end_time.minute,
                        tzinfo=ZoneInfo(tz_end),
                    ).strftime("%H:%M%z")
                )

        return start_times, end_times

    """ @staticmethod
    def get_duration(soup, direct_flights_mask: List[int]) -> List[int]:
        times = soup("div", class_="vmXl vmXl-mod-variant-default")
        durations = []
        for mask, t in zip(direct_flights_mask, times):
            if mask:
                duration = datetime.strptime(t.text, "%Hh %Mm")
                duration = duration.hour * 60 + duration.minute
                durations.append(duration)

        return durations """

    @staticmethod
    def get_direct_flights_mask(
        soup,
    ):
        direct_flights = soup.find_all("span", class_="JWEO-stops-text")
        direct_flights = [1 if "nonstop" in d.text else 0 for d in direct_flights]
        return direct_flights

    @staticmethod
    def get_flight_price(
        soup, direct_flights_mask: List[int]
    ) -> Tuple[List[float], List[str]]:
        prices_str = soup.find_all("div", class_="f8F1-price-text")
        prices = []
        currencies = []
        for mask, p in zip(direct_flights_mask, prices_str):
            if mask:
                price = float(p.text[1:])
                prices.append(price)
                currency = str(p.text[0])
                currencies.append(currency)
        return prices, currencies

    @staticmethod
    def get_timezone(place_name: str):
        geolocator = Nominatim(user_agent="tz_finder")
        location = geolocator.geocode(place_name)

        if location:
            finder = TimezoneFinder()
            zone = finder.timezone_at(lng=location.longitude, lat=location.latitude)
            return zone
        else:
            raise ValueError(f"Location {place_name} not found.")


def generate_date_range(start_date_str: str, end_date_str: str) -> List[str]:
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    current_date = start_date
    date_list = []
    while current_date <= end_date:
        date_list.append(str(current_date))
        current_date += timedelta(days=1)
    return date_list


@hydra.main(version_base="1.3", config_path="../scraper_configs", config_name="config")
def main(cfg: DictConfig):

    dir = os.listdir(cfg.get("output_data_dir"))
    if not cfg.get("run_once") or len(dir) == 0:
        scraper = Scraper(
            headless=cfg.get("headless"),
        )

        start_date = cfg.get("start_date")
        end_date = cfg.get("end_date")
        dates = generate_date_range(start_date, end_date)
        locations = list(str.split(cfg.get("locations"), ","))
        output_data_dir = cfg.get("output_data_dir")
        date_format = cfg.get("date_format")
        data_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".csv"
        iata_codes_mapping = {}

        with open(cfg.get("available_airports")) as f:
            iata_codes_mapping = json.load(f)

        scraper.scrape(
            output_data_dir,
            data_filename,
            date_format,
            dates,
            locations,
            iata_codes_mapping,
        )
    else:
        print("Data already exists, set `run_once` to false to scrape again.")


if __name__ == "__main__":
    main()
