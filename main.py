import os
import psycopg2  # type: ignore
from psycopg2 import sql
from meteo import get_data, save_data_in_db
from dotenv import load_dotenv  # type: ignore

load_dotenv()
db_url = os.getenv("DATABASE_URL")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def create_db(db_name: str, db_user: str, db_password: str) -> None:
    """create the database

    Parameters
    ----------
    db_name : str
        the name of the database
    db_user : str
        the user of the database
    db_password : str
        the password of the database
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user=db_user,
        password=db_password,
        host="localhost",
        port="5432",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    conn.close()


def create_table(db_url: str) -> None:
    """create the table in the database

    Parameters
    ----------
    db_url : str
        the url of the database
    """
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE meteo_cotedivoire (
            Date DATE,
            Temp_Max FLOAT,
            Temp_Min FLOAT,
            Temp_Moy FLOAT,
            Precip FLOAT,
            Wind FLOAT,
            Humidity FLOAT,
            Pressure FLOAT
        )
        """
    )
    conn.commit()
    conn.close()


create_db(db_name, db_user, db_password)

url = "https://www.historique-meteo.net/afrique"
# 2009 to 2024
years = list(range(2009, 2025))
df = get_data(url, years)
create_table(db_url)
save_data_in_db(df, db_url)
