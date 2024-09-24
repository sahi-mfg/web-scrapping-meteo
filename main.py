from src.meteo import get_data
from dotenv import load_dotenv  # type: ignore

# Load environment variables
load_dotenv()


def main():
    url = "https://www.historique-meteo.net/afrique/cote-d-ivoire/"
    # years = list(range(2020, 2025))
    years = [2024]
    df = get_data(url, years)
    return df


if __name__ == "__main__":
    data = main()
    print(data)
