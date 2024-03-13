from meteo import get_data


def main():
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire"
    years = [2024, 2022, 2021]
    df = get_data(url, years)

    return df


if __name__ == "__main__":
    df = main()
    df.to_csv("output/meteo.csv", index=False)
