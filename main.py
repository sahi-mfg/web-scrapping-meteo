from meteo import get_data


def main():
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire/"
    years = list(range(2020, 2025))
    #years = [2024]
    df = get_data(url, years)

    return df


if __name__ == "__main__":
    data = main()
    data.to_parquet("meteo.parquet", index=False)
    print(data)




