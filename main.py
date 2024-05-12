import asyncio
from meteo import get_data


url = "https://www.historique-meteo.net/afrique"
# 2009 to 2024
years = list(range(2009, 2025))

# create an event loop
loop = asyncio.get_event_loop()
# run the function in the event loop
df = loop.run_until_complete(get_data(url, years))
df.to_csv("output/as_meteo_data_civ.csv", index=False)
