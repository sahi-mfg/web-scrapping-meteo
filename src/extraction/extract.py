import logging
import os

from utils.webscrapping import get_data
from dotenv import load_dotenv  # type: ignore

from polars import DataFrame

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

years = [2024, 2023, 2022]
SCRAPPING_URL = os.getenv("SCRAPPING_URL")


def extract_data(years: list) -> DataFrame:
    logger.info("Extracting data from the web")
    df = get_data(SCRAPPING_URL, years)
    logger.info(f"Extraction done. {len(df)} lines extracted.")
    return df
