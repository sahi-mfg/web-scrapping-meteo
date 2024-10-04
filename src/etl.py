import os
from meteo import get_data
from dotenv import load_dotenv  # type: ignore
from polars import DataFrame
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime

# Load environment variables
load_dotenv()

years = [2024, 2023, 2022]

SCRAPPING_URL = os.getenv("SCRAPPING_URL")


def extract_data(url: str, years: list) -> DataFrame:
    df = get_data(url, years)
    return df


def conversion(df: DataFrame, column: str, char: str) -> DataFrame:
    df = df.with_column(column, df[column].str.replace(char, "").cast(float))
    return df


def transform_data(data: DataFrame) -> DataFrame:
    df = data.to_pandas()
    df = df.dropna()
    df = df.drop_duplicates()

    temp_cols = [col for col in data.columns if "temperature" in col]
    for col in temp_cols:
        df = conversion(df, col, "°")

    per_cols = ["humidite", "couverture-nuageuse"]
    for col in per_cols:
        df = conversion(df, col, "%")

    df = conversion(df, "pression", "hPa")
    df = conversion(df, "precipitations", "mm")
    df = conversion(df, "vitesse-vent", "km/h")
    df = conversion(df, "point-de-rosee", "°C")
    df = conversion(df, "visibilite", "km")
    df["indice-de-chaleur"] = df["indice-de-chaleur"].astype(float)
    df["Date"] = df["Date"].astype("datetime64[ns]")
    df.drop(columns=["duree-du-jour"])
    df = df._from_pandas(df)
    return df


def load_data(data: DataFrame, db_url: str, table_name: str) -> None:
    df = data.to_pandas()
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False)


def etl():
    extracted_data = extract_data(url=SCRAPPING_URL, years=years)
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data, os.getenv("DB_URL"), os.getenv("TABLE_NAME"))


# Définition du DAG Airflow
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 3),
    "retries": 1,
}

with DAG("weather_etl", default_args=default_args, schedule="@daily") as dag:
    etl_task = PythonOperator(task_id="run_etl", python_callable=etl)
