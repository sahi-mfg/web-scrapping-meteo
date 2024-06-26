"""
scraping historique-meteo.net to get meteo data from Côte d'Ivoire
@author: Sahi Gonsangbeu
"""

# ruff: max-line-length=120

import re
import unicodedata
from typing import Tuple, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs  # type: ignore
from tqdm import tqdm
import aiohttp  # type: ignore
import asyncio


def get_countries_urls(url: str) -> pd.DataFrame:
    """get urls for each country

    Parameters
    ----------
    url : str
        the url of the website

    Returns
    -------
    pd.DataFrame
        A dataframe with the urls and the name of each country
    """
    request = requests.get(url, timeout=5)
    soup = bs(request.text, "html.parser")
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
    request = requests.get(url, timeout=5)
    soup = bs(request.text, "html.parser")
    base = "https://www.historique-meteo.net"
    # extraction des régions
    city_tags = soup.find("div").find_all_next(
        "a", {"class": "list-group-item"}, href=True
    )
    # recupération des urls
    url_city = [base + tag["href"] for tag in city_tags]
    # récupération des noms des régions (des villes en fait)
    cities_names = [
        "".join(tag.get_attribute_list("title")).strip()[19:] for tag in city_tags
    ]
    infos = list(zip(url_city, cities_names))
    # on ne garde que les données par villes (on exclut celles par années)
    return infos[15:]


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


async def get_day_data(
    url: str, session: aiohttp.ClientSession
) -> tuple[list[str], list[str]]:
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
    async with session.get(url) as response:
        text = await response.text()
        soup = bs(text, "html.parser")
        # extraction des kpis
        kpis = soup.find("table").find_all("tr")[1:]
        kpis = [split_on_first_digit(kpi.get_text().strip())[0] for kpi in kpis]
        # suppression du dernier élement qui n'est pas utile pour nous ici
        kpis.pop()
        # Pour mettre dans le bon format avec slugify
        kpis = [slugify(kpi) for kpi in kpis]
        # extractions des valeurs des kpis
        values = soup.find("table").find_all("td", {"class": "text-center bg-primary"})[
            1:
        ]
        values = [value.get_text() for value in values]

        return kpis, values


async def get_data(url: str, years: Optional[List[int]] = None) -> pd.DataFrame:
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
    all_data = []

    async with aiohttp.ClientSession() as session:
        for year in years:
            if year == 2024:
                months_range = range(1, 4)
            else:
                months_range = range(1, 13)

            for month in months_range:
                for city_url, _ in cities:
                    days_range = (
                        range(1, 32)
                        if month in [1, 3, 5, 7, 8, 10, 12]
                        else range(1, 31)
                        if month != 2
                        else range(1, 29)
                    )

                    tasks = []
                    for day in tqdm(days_range, desc=f"{year}-{month}"):
                        task = asyncio.ensure_future(
                            get_day_data(
                                f"{city_url}/{year}/{month:02}/{day:02}", session
                            )
                        )
                        tasks.append(task)

                    responses = await asyncio.gather(*tasks)
                    for kpis, values in responses:
                        data = dict(zip(kpis, values))
                        data["Date"] = f"{year}/{month:02}/{day:02}"
                        all_data.append(data)

        df = pd.DataFrame(all_data)
        print(df)

        return df
