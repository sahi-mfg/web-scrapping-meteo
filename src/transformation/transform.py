import logging

import polars as pl
from polars import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_and_convert(
    df: DataFrame, columns: dict[str, tuple[str, pl.DataType]]
) -> DataFrame:
    """
    Clean and convert specified columns in the DataFrame.

    Args:
        df (DataFrame): Input DataFrame
        columns (dict[str, tuple[str, pl.DataType]]): Columns to clean and convert

    Returns:
        DataFrame: Cleaned and converted DataFrame
    """
    for col, (pattern, dtype) in columns.items():
        if col in df.columns:
            df = df.with_columns(
                [pl.col(col).str.replace(pattern, "").cast(dtype).alias(col)]
            )
    return df


def transform_data(data: DataFrame) -> DataFrame:
    """
    Transform the weather data DataFrame.

    Args:
        data (DataFrame): Input DataFrame

    Returns:
        DataFrame: Transformed DataFrame
    """
    # Define column transformations
    conversions = {
        "temperature-maximale": (r"°", pl.Float64),
        "temperature-minimale": (r"°", pl.Float64),
        "humidite": (r"%", pl.Float64),
        "couverture-nuageuse": (r"%", pl.Float64),
        "pression": (r"hPa", pl.Float64),
        "precipitations": (r"mm", pl.Float64),
        "vitesse-vent": (r"km/h", pl.Float64),
        "point-de-rosee": (r"°C", pl.Float64),
        "visibilite": (r"km", pl.Float64),
    }

    # Apply transformations
    df = (
        data.drop_nulls()
        .pipe(clean_and_convert, conversions)
        .with_columns(
            [
                pl.col("indice-de-chaleur").cast(pl.Float64),
                pl.col("Date").str.to_datetime(),
            ]
        )
        .drop("duree-du-jour")
    )

    return df
