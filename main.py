from meteo import get_data, get_day_data


def main():
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire/abidjan/2024/01/01/"
    #years = list(range(2020, 2025))
    data = get_day_data(url)
    print(data)
    #df = get_data(url, years)

    #return df


if __name__ == "__main__":
    main()
