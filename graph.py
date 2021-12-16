import datetime
from typing import Iterator
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import pandas as pd
import json
import numpy as np

from tqdm import tqdm

import time

import csv
import os

DIMS = {
    "group_dim": {
        "id": ["group_name"],
        "fields": ["group_name"],
        "type": "TerroristGroup",
        "relation_name": "GROUP_FACT",
        "source_type": "Group",
        "target_type": "Fact"
    },

    "location_dim": {
        "id": ["region", "country", "provstate", "city"],
        "fields": ["region", "country", "provstate", "city", "longitude", "latitude"],
        "type": "Location",
        "relation_name": "LOCATION_FACT",
        "source_type": "Location",
        "target_type": "Fact"
    },

    "date_dim": {
        "id": ["iyear", "imonth", "iday"],
        "fields": ["iyear", "imonth", "iday"],
        "type": "Date",
    },

    "target_dim": {
        "id": "target_id",
        "fields": ["target", "target_nat", "target_entity", "target_type"],
        "type": "Target"
    },

    "attack_dim": {
        "id": "attack_id",
        "fields": ["attack_type", "success", "suicide", "weapon_type"],
        "type": "Attack"
    },

    "dmg_dim": {
        "id": "damage_id",
        "fields": ["total_killed", "perps_killed", "total_wounded", "perps_wounded", "property_dmg", "property_dmg_value"],
        "type": "Damage"
    },

    "ransom_dim": {
        "id": "ransom_id",
        "fields": ["ransom", "ransom_amt", "ransom_amt_paid", "nreleased"],
        "type": "Ransom"
    },

    "hostage_dim": {
        "id": "hostage_id",
        "fields": ["host_kid", "nhost_kid", "host_kid_hours", "host_kid_days"],
        "type": "Hostage"
    },

    "attacker_dim": {
        "id": "attacker_id",
        "fields": ["individual", "nperps", "nperps_cap"],
        "type": "Attacker"
    }
}

FACT = {
    "fields": ["group_id", "location_id", "date_id", "target_id", "attack_id", "damage_id", "ransom_id", "hostage_id", "attacker_id"],
    "measures": ["tot_killed", "tot_deaths", "nhostages", "tot_prop_dmg", "tot_nreleased", "nkill_pop_ratio"],
    "type": "Fact"
}


DIM_RELATIONS = {
    "group_dim": {
        "relation_name": "GROUP_FACT",
        "source_type": "Group",
        "target_type": "Fact"
    },
}





class graph:
    def __init__(self, uri, gtd_datapath, city_population_path, country_population_path) -> None:
        self.driver = GraphDatabase.driver(uri)
        self.gtd_data = pd.read_csv(gtd_datapath)
        self.city_data = pd.read_csv(city_population_path)
        self.country_data = pd.read_csv(country_population_path)
    


if __name__ == "__main__":

    data = pd.read_csv("test.csv", nrows=10000)

    print(len(data.index))
    data = data.drop_duplicates()
    print(len(data.index))

    dfs = {}

    fact_df = pd.DataFrame(columns=FACT["fields"])

    for key in DIMS.keys():
        dfs[key] = (pd.DataFrame(columns=DIMS[key]["fields"]))
    
    for df in dfs.values():
        print(df.columns)

    for key, df in dfs.items():
        dfs[key] = data[df.columns]
    
    for key, df in dfs.items():
        print(key)
        print(df)

    test_list = [range(1,201),range(1,201),range(1,201)]
    import itertools
    output_list = list(itertools.product(*test_list))

    print(len(output_list))
    

    """
    dims = {}

    for key in DIMS:
        print(key, "--------------")

        dims[key] = []
        for i, d in data.iterrows():
            row = {}
            row["id"] = d[DIMS[key]["id"]]
            res = [d[field] for field in DIMS[key]["fields"]]
            for i, f in enumerate(DIMS[key]["fields"]):
                row[f] = res[i]

            print(row)
            dims[key].append(row)
    
    print(dims)
    
    print("Fact")
    for i, d in data.iterrows():
        res = [d[field] for field in FACT["fields"]]
        print(res)
    """