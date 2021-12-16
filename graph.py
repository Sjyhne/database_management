import datetime
from typing import Iterator
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import pandas as pd
import json
import numpy as np

import psycopg

from odb_data_extraction import SQL

import traceback

from create_and_load_dw import TIME_DIM_QUERY, LOCATION_DIM_QUERY, EVENT_DIM_QUERY, TARGET_DIM_QUERY, GROUP_DIM_QUERY

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
        "target_type": "Fact",
        "query": GROUP_DIM_QUERY
    },

    "location_dim": {
        "id": ["region", "country", "provstate", "city"],
        "fields": ["longitude", "latitude", "region", "country", "provstate", "city"],
        "type": "Location",
        "relation_name": "LOCATION_FACT",
        "source_type": "Location",
        "target_type": "Fact",
        "query": LOCATION_DIM_QUERY
    },

    "time_dim": {
        "id": ["year", "month", "day"],
        "fields": ["year", "month", "day"],
        "type": "Time",
        "query": TIME_DIM_QUERY
    },

    "target_dim": {
        "id": "target",
        "fields": ["target", "target_nat", "target_entity", "target_type"],
        "type": "Target",
        "query": TARGET_DIM_QUERY
    },

    "event_dim": {
        "id": "event_id",
        "fields": ["event_id", "attack_type", "success", "suicide", "weapon_type", "individual", "nperps", "nperps_cap", "host_kid", "nhost_kid", "host_kid_hours", "host_kid_days", "ransom", "ransom_amt", "ransom_amt_paid", "host_kid_outcome", "nreleased", "total_killed", "perps_killed", "total_wounded", "perps_wounded", "property_dmg", "property_dmg_value", "prop_dmg"],
        "type": "Event",
        "query": EVENT_DIM_QUERY
    },
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



class Graph:
    def __init__(self, uri) -> None:
        self.driver = GraphDatabase.driver(uri)
    
    def close_driver(self):
        self.driver.close()

    


if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"

    sql = SQL()

    g = Graph(bolt_url)
    
    dim_fields = DIMS["time_dim"]["fields"]
    dim_query = DIMS["time_dim"]["query"]

    start = time.time()
    for key in DIMS.keys():
        dim_fields = DIMS[key]["fields"]
        dim_query = DIMS[key]["query"]

        df = pd.DataFrame(columns=dim_fields)
        for r in sql.query_data(dim_query):
            df = df.append(dict(zip(dim_fields, r)), ignore_index=True)
        if key == "location_dim":
            df = df.drop_duplicates()
        df.to_csv(f"graph_data/{key}.csv")
    end = time.time()

    print("time taken:", end - start)