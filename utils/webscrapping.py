"""
scraping historique-meteo.net to get meteo data from Côte d'Ivoire
@author: Sahi Gonsangbeu
"""

# ruff: max-line-length=120

import re
import unicodedata
from typing import List, Optional, Tuple

import pandas as pd
import polars as pl
import requests
from bs4 import BeautifulSoup as bs  # type: ignore
from polars import DataFrame
from tqdm import tqdm  # type: ignore


def fetch_html_soup(url: str) -> bs:
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        soup = bs(response.text, "lxml")
        return soup
    except requests.exceptions.Timeout:
        print(f"Request to {url} timed out.")
    except requests.exceptions.RequestException as e:
        print(f"An error occured: {e}")
    return None


def get_countries_urls(url: str) -> DataFrame:
    """get urls for each country

    Parameters
    ----------
    url : str
        the url of the website

    Returns
    -------
    DataFrame
        A dataframe with the urls and the name of each country
    """
    soup = fetch_html_soup(url)
    base = "https://www.historique-meteo.net"
    # extraction des pays
    country_tags = soup.find_all("div", {"class": "item-text"})
    # récupération des urls pour chaque pays
    url_country_tags = soup.find_all("a", {"class": "hover-wrap fancybox"}, href=True)
    url_country = [base + tag["href"] for tag in url_country_tags]
    # récupération du nom de chaque pays
    country_name = [tag.get_text(strip=True) for tag in country_tags]
    infos = list(zip(url_country, country_name))
    df_country = pd.DataFrame(infos, columns=["Url", "Country"])
    df_country = pl.from_pandas(df_country)
    return df_country


def get_cities_url(url: str) -> List[Tuple[str, str]]:
    """get urls for each city of a country

    Parameters
    ----------
    url : str
        country's url

    Returns
    -------
    list
        A list of urls for each city
    """
    soup = fetch_html_soup(url)
    base = "https://www.historique-meteo.net"
    # extraction des régions
    city_tags = soup.find("div").find_all_next("a", {"class": "list-group-item"}, href=True)
    # recupération des urls
    url_city = [base + tag["href"] for tag in city_tags]
    # récupération des noms des régions (des villes en fait)
    cities_names = ["".join(tag.get_attribute_list("title")).strip()[19:] for tag in city_tags]
    infos = list(zip(url_city, cities_names))
    # on ne garde que les données par villes (on exclut celles par années)
    return infos[16:]


def slugify(string: str) -> str:
    """slugify a string

    Parameters
    ----------
    string : str
        the string to slugify

    Returns
    -------
    str
        the string slugified
    """
    # Normalize the string to remove accents and diacritics
    normalized_string = unicodedata.normalize("NFKD", string)
    # Replace non-alphanumeric characters with a hyphen
    slugify_string = re.sub(r"[^\w\s-]", "", normalized_string).strip().lower()
    # Replace spaces with a hyphen
    slugify_string = re.sub(r"[-\s]+", "-", slugify_string)
    return slugify_string


def split_on_first_digit(s: str) -> Tuple[str, str]:
    """split a string on the first digit

    Parameters
    ----------
    s : str
        the string to split

    Returns
    -------
    tuple
        the string split
    """
    for i, char in enumerate(s):
        if char.isdigit():
            return s[:i], s[i:]
    return s, ""


def get_day_data(url: str) -> dict[str, str]:
    """get data for a specific day

    Parameters
    ----------
    url : str
        the url

    Returns
    -------
    tuple
        A tuple containing the information and their values
    """
    soup = fetch_html_soup(url)
    # extraction des kpis
    kpis = soup.find("table").find_all("td", {"class": ""})[1:]
    kpis = [kpi.get_text().strip() for kpi in kpis]
    kpis = [slugify(kpi) for kpi in kpis]
    # extractions des valeurs des kpis
    values = soup.find("table").find_all("td", {"class": "text-center bg-primary"})[1:]
    values = [value.get_text() for value in values]
    return {kpi: value for kpi, value in zip(kpis, values)}


# TODO: Rendre l'exécution de ce code plus rapide avec la programmation asynchrone et stocker ces données dans un data lake


def get_data(url: str, years: Optional[List[int]] = None) -> DataFrame:
    """get data for a country and for the specified years

    Parameters
    ----------
    url : str
        the url of the country
    years : list, optional
        list containing the years, by default None

    Returns
    -------
    pd.DataFrame
         A dataframe containing the meteo data
    """
    # Retrieve all regions and their links for the chosen country
    if years is None:
        years = []
    cities = get_cities_url(url)
    # print(cities)
    all_data = []

    for year in years:
        # Stop at June 30th for the year 2024
        if year == 2024:
            months_range = range(1, 9)
        else:
            months_range = range(1, 13)

        for month in months_range:
            for city_url, city_name in cities:
                # Retrieve data for each day of the month
                days_range = (
                    range(1, 32) if month in [1, 3, 5, 7, 8, 10, 12] else range(1, 31) if month != 2 else range(1, 29)
                )

                for day in tqdm(
                    days_range,
                    desc=f"{city_name} {year}-{month:02}",
                    ascii=True,
                    ncols=100,
                ):
                    # Retrieve data for the specific day and region
                    data = get_day_data(f"{city_url}/{year}/{month:02}/{day:02}")
                    data["Date"] = f"{year}/{month:02}/{day:02}"
                    # Add the data to the list
                    all_data.append(data)

    # Create a DataFrame from the list of dictionaries
    df = DataFrame(all_data)

    return df
