from meteo import get_data


def main() -> None:
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire"
    years = [2024]
    df = get_data(url, years)

    df.to_csv("meteo_data.csv", index=False)


if __name__ == "__main__":
    main()
