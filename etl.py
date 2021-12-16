from typing import List
import pandas as pd
import numpy as np
import json
from datetime import datetime
from tqdm import tqdm


def compose_date(year: int, month: int, day: int):
    return datetime.strptime("/".join([str(year), str(month).zfill(2), str(day).zfill(2)]), "%Y/%m/%d").__str__()

def create_date(data: pd.DataFrame) -> pd.DataFrame:
    day, month, year = data["day"].to_numpy(), data["month"].to_numpy(), data["year"].to_numpy()

    for i, m in enumerate(month):
        if m == 0:
            month[i] = 1
    for i, d in enumerate(day):
        if d == 0:
            day[i] = 1

    dates = np.ndarray(len(year), dtype=object)
    
    for i in range(len(year)):
        dates[i] = compose_date(year[i], month[i], day[i])
    
    data["date"] = data["date"] = dates

    return data

def comma_float_to_point_float(field):
    if type(field) == str:
        return float(field.replace(",", "."))
    else:
        return field

def fix_coords(data: pd.DataFrame) -> pd.DataFrame:
    cols = ["longitude", "latitude"]

    for col in cols:
        data[col] = data[col].apply(comma_float_to_point_float)
    
    return data

def print_data_statistics(data: pd.DataFrame):
    print(data.describe(include="all"))

def get_cols(cols_path: str) -> List:
    cols = []
    with open(cols_path, "r") as f:
        for line in f.readlines():
            if line[0] != "-":
                cols.append(line.strip("").replace("\n", ""))
    
    return cols

def get_data(data_path: str, cols: List, rename_cols: List, nrows: int=None) -> pd.DataFrame:
    if nrows == None:
        return pd.read_csv(data_path, usecols=cols).rename(columns=rename_cols)
    else:
        return pd.read_csv(data_path, usecols=cols, nrows=nrows).rename(columns=rename_cols)


def extract(data_path: str, columns_path: str, rename_columns_path: str, nrows: int = None) -> pd.DataFrame:
    cols = get_cols(columns_path)
    rename_cols = json.load(open(rename_columns_path))
    return get_data(data_path, cols, rename_cols, nrows)


def to_int(field):
    return int(float(field))

def neg_to_nan(field):
    if not pd.isna(field) and to_int(field) < 0:
        return 0
    elif pd.isna(field):
        return 0
    elif field == np.nan:
        return 0
    else:
        return field

def float_to_int(field):
    if type(field) != str:
        if type(field) == float and not pd.isna(field):
            return to_int(field)
        elif pd.isna(field):
            return 0
        else:
            return field
    else:
        return field

def floatify_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    columns = [col for col in data.columns]

    for col in columns:
        data[col] = data[col].apply(float_to_int)
    
    return data

def nanify_negative_numbers(data: pd.DataFrame) -> pd.DataFrame:
    neg_cols = ["nperps_cap", "nperps", "property_dmg"]

    for col in neg_cols:
        data[col] = data[col].apply(neg_to_nan)
    
    return data

def replace_comma_with_dot(field):
    if type(field) == str:
        if "," in field:
            return field.replace(",", ".")
        else:
            return field
    else:
        return field

def remove_commas(data: pd.DataFrame) -> pd.DataFrame:
    cols = [col for col in data.columns]
    for col in cols:
        data[col] = data[col].apply(replace_comma_with_dot)
    
    return data
    
def add_current_countries(data: pd.DataFrame) -> pd.DataFrame:
    lats, longs = data["latitude"], data["longitude"]
    data["current_country"] = pd.Series.empty

    print(len(lats), len(longs))
    latlongs = []
    for i in range(len(lats)):
        latlongs.append(str(lats[i].round(3)) + "," + str(longs[i].round(3)))
    
    long_lat_map = json.loads(open("etl_data/corrected_long_lat_map.json", "r").read())

    for _, i in tqdm(enumerate(range(len(latlongs))), total=len(latlongs), desc="Reverse geocoding"):
        try:
            country = long_lat_map[latlongs[i]]
            if country == None:
                country = "Unknown"
        except:
            country = "Unknown"
        data["current_country"][i] = country

    return data

def remove_apostrophe(field):
    if type(field) == str:
        field = field.replace("'", "").replace('"', "")
        return field
    else:
        return field
    
def remove_all_apostrophes(data: pd.DataFrame) -> pd.DataFrame:
    data_cols = data.columns
    for col in data_cols:
        data[col] = data[col].apply(remove_apostrophe)
    
    return data

def unknown_if_not_string(field):
    if type(field) != str:
        return "Unknown"
    else:
        return field

def remove_integers_from_stringfield(data: pd.DataFrame) -> pd.DataFrame:

    cols = ["country", "target_nat", "target_sub_type", "target_entity", "target", "weapon_sub_type", "prop_dmg", "host_kid_outcome"]

    for col in cols:
        data[col] = data[col].apply(unknown_if_not_string)
    
    return data

def stringify_event_id(data: pd.DataFrame) -> pd.DataFrame:
    data["event_id"] = data["event_id"].apply(str)
    return data

def transform(data_path: str, columns_path: str, rename_columns_path: str, nrows: int = None) -> pd.DataFrame:
    data = extract(data_path, columns_path, rename_columns_path, nrows)

    print("(1/8) - Removing commas")
    data = remove_commas(data)

    print("(2/8) - Floatifying dataframe")
    data = floatify_dataframe(data)

    print("(3/8) - Nanifying negative numbers")
    data = nanify_negative_numbers(data)

    print("(4/8) - Creating dates")
    data = create_date(data)

    print("(5/8) - Fixing coords")
    data = fix_coords(data)

    print("(6/8) - Adding current countries")
    data = add_current_countries(data)

    print("(7/8) - Removing all apostrophes from strings")
    data = remove_all_apostrophes(data)

    print("(8/8) - Removing integers from country cols")
    data = remove_integers_from_stringfield(data)

    for col in data.columns:
        print("\n")
        print(col.upper())
        print(col, "-", sorted(data[col].unique()))

    return data

#get_current_country(40.714224, -73.961452)


res = transform("etl_data/gtd.csv", "etl_data/datacols.txt", "etl_data/rename_cols.json", 200)


res.to_csv("test.csv")