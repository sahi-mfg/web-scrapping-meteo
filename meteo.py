import requests
import re
import unicodedata

from bs4 import BeautifulSoup as bs  # type: ignore
import pandas as pd
from tqdm import tqdm


def get_countries_urls(url: str) -> pd.DataFrame:
    """get urls for each countries

    Parameters
    ----------
    url : str
        the url of the website

    Returns
    -------
    pd.DataFrame
        A dataframe with the urls and the name of each country
    """
    request = requests.get(url)
    soup = bs(request.text, "html.parser")
    base = "https://www.historique-meteo.net"
    # extraction des pays
    country_tags = soup.find_all("div", {"class": "item-text"})
    # récupération des urls pour chaque pays
    url_country_tags = soup.find_all("a", {"class": "hover-wrap fancybox"}, href=True)
    url_country = [base + tag["href"] for tag in url_country_tags]
    # récupération du nom de chaque pays
    country_name = [tag.get_text(strip=True) for tag in country_tags]
    infos = [(url, nom) for (url, nom) in zip(url_country, country_name)]
    df_country = pd.DataFrame(infos, columns=["Url", "Country"])
    return df_country


def get_cities_url(url: str) -> list:
    """get urls for each cities of a country

    Parameters
    ----------
    url : str
        country's url

    Returns
    -------
    list
        list of urls for each cities
    """
    request = requests.get(url)
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
    infos = [(url, nom) for (url, nom) in zip(url_city, cities_names)]
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
    slugified_string = re.sub(r"[^\w\s-]", "", normalized_string).strip().lower()
    # Replace spaces with a hyphen
    slugified_string = re.sub(r"[-\s]+", "-", slugified_string)
    return slugified_string


def split_on_first_digit(s: str) -> tuple:
    """split a string on the first digit

    Parameters
    ----------
    s : str
        the string to split

    Returns
    -------
    tuple
        the string splitted
    """
    for i, char in enumerate(s):
        if char.isdigit():
            return s[:i], s[i:]
    return s, ""


def get_day_data(url: str) -> tuple:
    """get data for a specific day

    Parameters
    ----------
    url : str
        the url

    Returns
    -------
    tuple
        tuple containing the infos and their values
    """
    request = requests.get(url)
    soup = bs(request.text, "html.parser")
    # extraction des kpis
    kpis = soup.find("table").find_all("tr")[1:]
    kpis = [split_on_first_digit(kpi.get_text().strip())[0] for kpi in kpis]
    # suppression du dernier élement qui n'est pas utile pour nous ici
    kpis.pop()
    # Pour mettre dans le bon format avec slugify
    kpis = [slugify(kpi) for kpi in kpis]
    # extractions des valeurs des kpis
    values = soup.find("table").find_all("td", {"class": "text-center bg-primary"})[1:]
    values = [value.get_text() for value in values]
    return (kpis, values)


def get_data(url: str, years: list[int] = []) -> pd.DataFrame:
    """get data for a country and for the specified years

    Parameters
    ----------
    url : str
        the url of the country
    years : list, optional
        list containing the years, by default []

    Returns
    -------
    pd.DataFrame
        dataframe containing the meteo data
    """
    # Retrieve all regions and their links for the chosen country
    cities = get_cities_url(url)
    all_data = []

    for year in years:
        # Stop at June 30th for the year 2022
        if year == 2022:
            months_range = range(1, 7)
        else:
            months_range = range(1, 13)

        for month in months_range:
            for city_url, city_name in cities:
                # Retrieve data for each day of the month
                days_range = (
                    range(1, 32)
                    if month in [1, 3, 5, 7, 8, 10, 12]
                    else range(1, 31)
                    if month != 2
                    else range(1, 29)
                )

                for day in tqdm(days_range, desc=f"{year}-{month}"):
                    # Retrieve data for the specific day and region
                    kpis, values = get_day_data(
                        f"{city_url}/{year}/{month:02}/{day:02}"
                    )
                    data = dict(zip(kpis, values))
                    data["Date"] = f"{year}/{month:02}/{day:02}"
                    # Add the data to the list
                    all_data.append(data)

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(all_data)

    return df
