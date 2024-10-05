import os

import psycopg2.extras as p
from src.extraction.extract import extract_data
from src.transformation.transform import transform_data
from utils.config import get_warehouse_creds
from utils.db import DBConnection, WarehouseConnection

years = [2024, 2023, 2022]
SCRAPPING_URL = os.getenv("SCRAPPING_URL")

# Load warehouse credentials
creds = get_warehouse_creds()
# Connect to the warehouse
db = DBConnection(
    user=creds.user,
    password=creds.password,
    db=creds.db,
    host=creds.host,
    port=creds.port,
)

# extract data
df = extract_data(SCRAPPING_URL, years)

# transform data
df = transform_data(df)


# load data into the warehouse
def load_data():
    data = df.to_pandas()
    with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
        p.execute_values(
            curr, f"INSERT INTO {os.getenv('TABLE_NAME')} VALUES %s", data.values
        )


if __name__ == "__main__":
    load_data()
