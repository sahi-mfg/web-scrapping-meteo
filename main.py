from meteo import get_data
from sqlalchemy import create_engine


def main():
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire"
    years = [2022]
    df = get_data(url, years)

    return df


if __name__ == "__main__":
    df = main()
    engine = create_engine("sqlite:///output/meteo_data_civ.db", echo=False)
    df.to_sql("meteo_data_civ", con=engine, if_exists="replace", index=False)
