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
    times = soup("div", class_="vmXl vmXl-mod-variant-large")

    # Find all span tags and extract the text
    # Parse it as datetime in the format %I:%M %p (12-hour format)
    # Convert it to 24-hour format
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
    direct_flights = soup.find_all("span", class_="JWEO-stops-text")
    direct_flights = [1 if "nonstop" in d.text else 0 for d in direct_flights]
    return direct_flights


def get_flight_price(
    soup: BeautifulSoup, direct_flights_mask: List[int]
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


def generate_permutations(
    dates: List[str], locations: List[str]
) -> List[Tuple[str, str, str]]:
    # Generate all permutations of locations
    location_permutations = list(itertools.permutations(locations, 2))
    # Generate all permutations of dates and location pairs
    permutations = [
        (date, loc1, loc2) for date in dates for loc1, loc2 in location_permutations
    ]
    return permutations


def save_info(
    dest_dir: str,
    filename: str,
    results: List[List[Any]],
):
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
