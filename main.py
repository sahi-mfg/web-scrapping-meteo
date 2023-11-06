from meteo import get_data


def main():
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire"
    years = [2020, 2021, 2022]
    df = get_data(url, years)
    return df


if __name__ == "__main__":
    df = main()
    print(df)
