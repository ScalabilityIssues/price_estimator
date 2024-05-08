import csv
from typing import Any, List, Tuple
from zoneinfo import ZoneInfo

from datetime import datetime, timedelta
import itertools
from geopy.geocoders import Nominatim
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

from bs4 import BeautifulSoup


def get_flight_times(
    soup: BeautifulSoup, direct_flights_mask: List[int], tz_start: str, tz_end: str
) -> Tuple[List[str], List[str]]:
    """
    Extracts the start and end times of flights from the given BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the flight information.
        direct_flights_mask (List[int]): A list of binary values indicating whether each flight is direct or not.
        tz_start (str): The timezone of the departure location.
        tz_end (str): The timezone of the destination location.

    Returns:
        Tuple[List[str], List[str]]: A tuple containing two lists - the start times and end times of the flights.
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
    Determines whether each flight in the given BeautifulSoup object is a direct flight or not.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the flight information.

    Returns:
        List[int]: A list of binary values indicating whether each flight is direct or not.
    """
    direct_flights = soup.find_all("span", class_="JWEO-stops-text")
    direct_flights = [1 if "nonstop" in d.text else 0 for d in direct_flights]
    return direct_flights


def get_flight_price(
    soup: BeautifulSoup, direct_flights_mask: List[int]
) -> Tuple[List[float], List[str]]:
    """
    Extracts the prices and currencies of flights from the given BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the flight information.
        direct_flights_mask (List[int]): A list of binary values indicating whether each flight is direct or not.

    Returns:
        Tuple[List[float], List[str]]: A tuple containing two lists - the prices and currencies of the flights.
    """
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
):
    """
    Saves flight information to a CSV file.

    Args:
        dest_dir (str): The destination directory where the file will be saved.
        filename (str): The name of the file.
        results (List[List[Any]]): A list of flight information.

    Returns:
        None
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
