import os

from meteo import get_data, transform_data
from sqlalchemy import create_engine
from dotenv import load_dotenv  # type: ignore

# Load environment variables
load_dotenv()


def main():
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire/"
    years = list(range(2020, 2025))
    # years = [2024]
    df = get_data(url, years)
    df = transform_data(df)
    return df


if __name__ == "__main__":
    data = main()
    print(data)

    # Get database connection details from environment variables
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    # Create a database connection
    engine = create_engine(
        f"postgresql://{db_username}:{db_password}@{db_host}/{db_name}"
    )

    # Save the data to the data warehouse
    data.to_sql("meteo_data", engine, if_exists="replace", index=False)

    print("Data successfully saved to the data warehouse.")
