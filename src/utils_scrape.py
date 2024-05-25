import asyncio
import csv
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo
import traceback as tb

from datetime import datetime, timedelta
import itertools
import geopy.geocoders
from geopy.geocoders import Nominatim
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from playwright.async_api import Browser

from bs4 import BeautifulSoup


def get_flight_times(
    soup: BeautifulSoup, direct_flights_mask: List[bool], tz_start: str, tz_end: str
) -> Tuple[List[str], List[str]]:
    """
    Get the start and end times of flights.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML.
        direct_flights_mask (List[bool]): A list of boolean values indicating whether each flight is a direct flight or not.
        tz_start (str): The timezone of the source location.
        tz_end (str): The timezone of the destination location.

    Returns:
        Tuple[List[str], List[str]]: A tuple containing the start times and end times of the flights.
    """
    times = soup("div", class_="vmXl vmXl-mod-variant-large")

    start_times = []
    end_times = []
    for mask, s in zip(direct_flights_mask, times):
        if mask:
            start_time = datetime.strptime(str(s.find_all("span")[0].text), "%I:%M %p")
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

            # print("BUG: ", s.find_all("span")[2].text)
            end_time = datetime.strptime(str(s.find_all("span")[2].text), "%I:%M %p")
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


def get_direct_flights_mask(
    soup: BeautifulSoup,
):
    """
    Get a mask indicating whether each flight is a direct flight or not. Flights that arrives are excluded by default.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML.

    Returns:
        list: A list of boolean values indicating whether each flight is a direct flight or not.
    """
    direct_flights = soup.find_all("span", class_="JWEO-stops-text")
    times = soup("div", class_="vmXl vmXl-mod-variant-large")
    day_after_flights = [
        True if "+1" in str(t.find_all("span")[2].text) else False for t in times
    ]
    direct_flights = [
        True if ("nonstop" in d_f.text) and (not d_a) else False
        for d_f, d_a in zip(direct_flights, day_after_flights)
    ]
    return direct_flights


def get_flight_price(
    soup: BeautifulSoup, direct_flights_mask: List[bool]
) -> Tuple[List[float], List[str]]:
    """
    Get the prices and currencies of flights.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the parsed HTML.
        direct_flights_mask (List[bool]): A list of boolean values indicating whether each flight is a direct flight or not.

    Returns:
        Tuple[List[float], List[str]]: A tuple containing the prices and currencies of the flights.
    """
    prices_str = soup.find_all("div", class_="f8F1-price-text")
    prices = []
    currencies = []
    for mask, p in zip(direct_flights_mask, prices_str):
        if mask:
            # remove comma that stands for thousands separator and dollar sign
            price = float(p.text[1:].replace(",", ""))
            prices.append(price)
            currency = str(p.text[0])
            currencies.append(currency)
    return prices, currencies


def get_timezone(place_name: str):
    """
    Retrieves the timezone of a given location using its name.

    Args:
        place_name (str): The name of the location.

    Returns:
        str: The timezone of the location.

    Raises:
        ValueError: If the location is not found.
    """
    geopy.geocoders.options.default_timeout = 120000
    geolocator = Nominatim(user_agent="tz_finder")
    location = geolocator.geocode(place_name)

    if location:
        finder = TimezoneFinder()
        zone = finder.timezone_at(lng=location.longitude, lat=location.latitude)
        return zone
    else:
        raise ValueError(f"Location {place_name} not found.")


def generate_date_range(start_date_str: str, end_date_str: str) -> List[str]:
    """
    Generates a list of dates within a given date range.

    Args:
        start_date_str (str): The start date of the range in the format "YYYY-MM-DD".
        end_date_str (str): The end date of the range in the format "YYYY-MM-DD".

    Returns:
        List[str]: A list of dates within the specified range.
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    current_date = start_date
    date_list = []
    while current_date <= end_date:
        date_list.append(str(current_date))
        current_date += timedelta(days=1)
    return date_list


def generate_permutations(
    dates: List[str], locations: List[str]
) -> List[Tuple[str, str, str]]:
    """
    Generates all possible permutations of dates and locations.

    Args:
        dates (List[str]): A list of dates.
        locations (List[str]): A list of locations.

    Returns:
        List[Tuple[str, str, str]]: A list of tuples representing the permutations of dates and locations.
    """
    location_permutations = list(itertools.permutations(locations, 2))
    permutations = [
        (date, loc1, loc2) for date in dates for loc1, loc2 in location_permutations
    ]
    return permutations


def save_info(
    dest_dir: str,
    results: List[List[Any]],
) -> str:
    """
    Saves flight information to a CSV file.

    Args:
        dest_dir (str): The destination directory where the file will be saved.
        results (List[List[Any]]): A list of flight information.

    Returns:
        str: The filename of the saved CSV file.
    """
    filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".csv"
    with open(dest_dir + filename, "x", newline="") as f:
        writer = csv.writer(f, delimiter=";")
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
        for (
            date,
            source,
            destination,
            start_times,
            end_times,
            prices,
            currencies,
        ) in results:
            for flight_data in zip(start_times, end_times, prices, currencies):
                row = [
                    date,
                    source,
                    destination,
                ] + list(flight_data)
                writer.writerow(row)
    return filename


async def scrape(
    task_id: int,
    browser: Browser,
    date: str,
    source: str,
    destination: str,
    iata_codes_mapping: Dict[str, str],
    locale: str = "en-US",
    prefix_url: str = "https://www.kayak.com/flights/",
    timeout: int = 120000,
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
                await asyncio.sleep(10)
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
            f"TASK {task_id} - source: {source} destination: {destination} date: {date}\n"
            + f"prices: {len(prices)} start_times: {len(start_times)} end_times: {len(end_times)} currencies: {len(currencies)}\n"
        )
        return [date, source, destination, start_times, end_times, prices, currencies]
    except Exception as e:
        print(f"Error scraping data from {url}")
        tb.print_exc()
    finally:
        await context.close()
    return []
