from meteo import get_data, get_day_data


def main() -> None:
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire"
    years = [2024]
    df = get_data(url, years)

    df.to_csv("meteo_data.csv", index=False)


if __name__ == "__main__":
    print(
        get_day_data(
            "https://www.historique-meteo.net/afrique/cote-d-ivoire/abidjan/2024/01/01"
        )
    )
    # main()
