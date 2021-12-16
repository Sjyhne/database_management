from typing import List, Tuple
import requests
import json
import pandas as pd

from tqdm import tqdm

CITY_URL = "https://countriesnow.space/api/v0.1/countries/population/cities"
COUNTRY_URL = "https://countriesnow.space/api/v0.1/countries/population"

def post_request(data: dict, url: str) -> List:
    x = requests.post(url, data=data)
    return x.text

def get_pop(data: dict, url: str, year_span: Tuple) -> dict:
    response = post_request(data, url)
    try:
        load = json.loads(response)["data"]["populationCounts"]
        load = {l["year"]: l for l in load}
    except:
        load = {}
    
    populations = {}

    for i in range(year_span[0], year_span[1] + 1):
        if i in load:
            populations[i] = load[i]["value"]
        else:
            populations[i] = 0

    
    return populations


def get_populations(locations: List, location_type: str, year_span: Tuple) -> dict:
    
    populations = {}

    url = CITY_URL if location_type == "city" else COUNTRY_URL

    locations = list(set(locations))
    
    for _, location in tqdm(enumerate(locations), total=len(locations), desc=location_type):
        pop = get_pop({location_type: location}, url, year_span)
        populations[location] = pop

    return populations

def get_newest_and_oldest(data: pd.DataFrame) -> Tuple:
    if "date" in data:
        dates = data["date"].to_numpy()
        dates = sorted(dates)
        return (int(dates[0].split("-")[0]), int(dates[-1].split("-")[0]))

def make_populations_csv(data: pd.DataFrame) -> pd.DataFrame:

    location_types = ["city", "country"]

    year_span = get_newest_and_oldest(data)

    for loc_type in location_types:

        locations = list(set(data[loc_type].to_numpy()))
        location_populations = get_populations(locations, loc_type, year_span)
        location_pop = pd.DataFrame(columns=[loc_type, "year", "population"])

        for loc, value in location_populations.items():
            for year, population in value.items():
                location_pop = location_pop.append({loc_type: loc, "year": year, "population": population}, ignore_index=True)
        
        location_pop.to_csv(f"{loc_type}_{data.size}_{year_span[0]}_{year_span[1]}.csv")


if __name__ == "__main__":
    data = pd.read_csv("full.csv")

    make_populations_csv(data)