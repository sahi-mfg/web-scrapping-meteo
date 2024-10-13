import logging

from polars import DataFrame
from utils.webscrapping import get_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_data(url: str, years: list) -> DataFrame:
    """extract Côte d'ivoire's meteo data from the web for the specified years

    Args:
        years (list): years to extract

    Returns:
        DataFrame: A DataFrame containing the extracted data
    """
    logger.info("Extracting data from the web")
    df = get_data(url, years)
    logger.info(f"Extraction done. {len(df)} lines extracted.")
    return df
