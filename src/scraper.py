from typing import List, Tuple
import hydra, csv, os
from datetime import datetime, timedelta
from time import sleep
import chromedriver_autoinstaller
import itertools
from bs4 import BeautifulSoup
from omegaconf import DictConfig
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Scraper:
    def __init__(self, prefix_url: str = "https://www.kayak.com/flights/"):
        self.prefix_url = prefix_url
        # check if chrome driver is installed correctly and on path
        chromedriver_autoinstaller.install()

        self.options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome(options=self.options)

    def __del__(self):
        self.driver.quit()

    def _get_website_info(
        self, url: str
    ) -> Tuple[List[str], List[str], List[float], List[str]]:

        print(f"Scraping data from {url}")
        self.driver.get(url)
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
                """ input("Captcha detected. Press any key to continue...")
                element = WebDriverWait(self.driver, 20).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//*[contains(@class, 'show-more-button')]")
                    )
                )
                element.click() """
            sleep(5)

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        direct_flights_mask = Scraper.get_direct_flights_mask(soup)
        start_times, end_times = Scraper.get_flight_times(soup, direct_flights_mask)
        prices, currencies = Scraper.get_flight_price(soup, direct_flights_mask)
        print(
            f"Scraping summary - prices: {len(prices)} start_times: {len(start_times)} end_times: {len(end_times)}"
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
                row = [date, source, destination] + list(row_data)
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

            start_times, end_times, prices, currencies = self._get_website_info(url)
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
        soup, direct_flights_mask: List[int]
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
                ).strftime("%H:%M")
                start_times.append(start_time)

                end_time = datetime.strptime(
                    str(s.find_all("span")[2].text), "%I:%M %p"
                ).strftime("%H:%M")
                end_times.append(end_time)

        return start_times, end_times

    """ @staticmethod
    def get_total_stops(soup):
        stops = soup.find_all('span',class_='JWEO-stops-text')
        stops=[0 if s.text == 'nonstop' else int(s.text[0:2]) for s in stops]
        return stops
    @staticmethod
    def compute_duration(start_time, end_time):
        start_time = datetime.strptime(start_time, "%H:%M")
        end_time = datetime.strptime(end_time, "%H:%M")
        duration = end_time - start_time
        return duration """

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
        scraper = Scraper()

        start_date = cfg.get("start_date")
        end_date = cfg.get("end_date")
        dates = generate_date_range(start_date, end_date)
        locations = list(str.split(cfg.get("locations"), ","))
        output_data_dir = cfg.get("output_data_dir")
        date_format = cfg.get("date_format")
        data_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".csv"

        scraper.scrape(output_data_dir, data_filename, date_format, dates, locations)
    else:
        print("Data already exists, set `run_once` to false to scrape again.")


if __name__ == "__main__":
    main()
