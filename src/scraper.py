from datetime import datetime, timedelta
from time import sleep
import chromedriver_autoinstaller
import itertools
from bs4 import BeautifulSoup
import hydra
import csv
from os.path import isfile
from omegaconf import DictConfig, OmegaConf
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class Scraper():
    def __init__(self, prefix_url="https://www.kayak.com/flights/", headless=True):
        self.prefix_url=prefix_url

        chromedriver_autoinstaller.install() # check if geckodriver is installed correctly and on path
        self.options = Options()
        self.options.headless = headless
        self.driver = webdriver.Chrome(options=self.options)
        
    def __del__(self):
        self.driver.quit()

    def _get_info(self, url):
            self.driver.get(url)

            sleep(2)
            # click show more button to get all flights
            for i in range(2):
                try:
                    element = WebDriverWait(self.driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//*[contains(@class, 'show-more-button')]")))
                    element.click()
                except Exception as e:
                    input("Captcha detected. Press any key to continue...")
                    element = WebDriverWait(self.driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//*[contains(@class, 'show-more-button')]")))
                    element.click()
                sleep(5)

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            start_times, end_times = Scraper.get_times(soup)
            prices = Scraper.get_price(soup)
            durations = [Scraper.compute_duration(start_time, end_time) for start_time, end_time in zip(start_times, end_times)]
            print("Flight info - ", "prices: ", len(prices), ", durations: ", len(durations), ", start_times: ", len(start_times), ", end_times: ", len(end_times))

            return start_times, end_times, prices, durations
            
            
    def _save_info(self, output_data_dir, data_filename, date, source, destination, start_times, end_times, prices, durations):
        """
        Save the date to a csv file
        """
        filename = data_filename
        file_exists = isfile(output_data_dir+filename)

        with open(output_data_dir+filename, 'a', newline='') as f:
            writer = csv.writer(f, delimiter=";")
            if not file_exists:
                writer.writerow(["date", "source", "destination", "start_time", "end_time", "price", "duration"])
            for data in zip(start_times, end_times, prices, durations):
                data = [date, source, destination] + list(data)
                writer.writerow(data)


    @staticmethod
    def _generate_permutations(dates, locations):
        # Generate all permutations of locations
        location_permutations = list(itertools.permutations(locations, 2))
        # Generate all permutations of dates and location pairs
        permutations = [(date, loc1, loc2) for date in dates for loc1, loc2 in location_permutations]
        return permutations
    

    def scrape(self, output_data_dir, data_filename, date_format, dates, locations):
        permuts=Scraper._generate_permutations(dates, locations)
        print("Scraping data...")
        for i, permut in enumerate(permuts):
            print("Iteration: ", i)
            date, source, destination = permut
            try:
                datetime.strptime(date, date_format)
            except ValueError:
                print(f"Date {date} is not in format {date_format}")
                continue

            # close and open driver every 10 days to avoid captcha
            if i!=0 and i % 5 == 0:
                self.driver.quit()
                self.driver = webdriver.Chrome(options=self.options)

            url = f"{self.prefix_url}{source}-{destination}/{date}?stops=~0&sort=bestflight_a"

            start_times, end_times, prices, durations = self._get_info(url)
            self._save_info(output_data_dir, data_filename, date, source, destination, start_times, end_times, prices, durations)
            print("Saved data to csv file\n")


    @staticmethod
    def get_times(soup):
        times = soup('div',class_='vmXl vmXl-mod-variant-large')
        start_times = [datetime.strptime(str(s.find_all('span')[0].text), "%I:%M %p").strftime("%H:%M") for s in times]
        end_times = [datetime.strptime(str(s.find_all('span')[2].text), "%I:%M %p").strftime("%H:%M") for s in times]
        
        return start_times, end_times
    
    """ @staticmethod
    def get_total_stops(soup):
        stops = soup.find_all('span',class_='JWEO-stops-text')
        stops=[0 if s.text == 'nonstop' else int(s.text[0:2]) for s in stops]
        return stops """

    @staticmethod
    def get_price(soup):
        prices = soup.find_all('div',class_='f8F1-price-text')
        prices=[float(p.text[1:]) for p in prices]
        return prices

    @staticmethod
    def compute_duration(start_time, end_time):
        start_time = datetime.strptime(start_time, "%H:%M")
        end_time = datetime.strptime(end_time, "%H:%M")
        duration = end_time - start_time
        return duration


def generate_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    current_date = start_date
    date_list = []
    while current_date <= end_date:
        date_list.append(str(current_date))
        current_date += timedelta(days=1)
    return date_list


@hydra.main(version_base=None, config_path="../configs", config_name="config")
def main(cfg: DictConfig):
    scraper = Scraper()
       
    start_date=cfg.get("start_date")
    end_date=cfg.get("end_date")
    dates=generate_date_range(start_date, end_date)
    locations=list(str.split(cfg.get("locations"), " "))
    output_data_dir=cfg.get("output_data_dir")
    date_format=cfg.get("date_format")
    data_filename=cfg.get("data_filename")

    scraper.scrape(output_data_dir, data_filename, date_format, dates, locations)
     

if __name__ == "__main__":
    main()
