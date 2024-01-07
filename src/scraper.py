from datetime import datetime
import chromedriver_autoinstaller
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

date_format = "%Y-%m-%d"

class Scraper():
    def __init__(self, prefix_url="https://www.kayak.com/flights/", headless=True):
        self.prefix_url=prefix_url

        chromedriver_autoinstaller.install() # check if geckodriver is installed correctly and on path
        self.options = Options()
        self.options.headless = headless
        self.driver = webdriver.Chrome(options=self.options)
        
    def __del__(self):
        self.driver.quit()


    def get_info(self, url):
            self.driver.get(url)
            # click show more button to get all flights
            """  try:
                # Wait for the element with a class containing "show-more-button" to be clickable
                show_more_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[contains(@class, "show-more-button")]'))
                )

                # Click the "show more" button
                show_more_button.click()
            except:
                # in case a captcha appears, require input from user so that the for loop pauses and the user can continue the
                # loop after solving the captcha
                input("Please solve the captcha then enter anything here to resume scraping.")
            """
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            start_times, end_times = Scraper.get_times(soup)
            stops = Scraper.get_total_stops(soup)
            prices = Scraper.get_price(soup)
            durations = [Scraper.compute_duration(start_time, end_time) for start_time, end_time in zip(start_times, end_times)]
            
            

    def scrape(self, dates, sources, destinations, bidirectional=True):
        for i, date in tqdm(enumerate(dates)):
            # close and open driver every 10 days to avoid captcha
            """ if i % 5 == 0:
                self.driver.quit()
                self.driver = webdriver.Chrome(options=self.options) """
            url = f"{self.prefix_url}{sources[i]}-{destinations[i]}/{date}"
            url_rev = f"{self.prefix_url}{destinations[i]}-{sources[i]}/{date}"
            self.get_info(url)
            if bidirectional:
                self.get_info(url_rev)
            

    @staticmethod
    def get_times(soup):
        times = soup('div',class_='vmXl vmXl-mod-variant-large')
        start_times = [str(s.find_all('span')[0].text)[:-3] for s in times]
        end_times = [str(s.find_all('span')[2].text)[:-3] for s in times]
        return start_times, end_times
    
    @staticmethod
    def get_total_stops(soup):
        stops = soup.find_all('span',class_='JWEO-stops-text')
        stops=[0 if s.text == 'nonstop' else int(s.text[0:2]) for s in stops]
        return stops

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


Scraper().scrape(["2024-03-01"], ["VCE"], ["PAR"])