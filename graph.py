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

import os

from create_and_load_dw import TIME_DIM_QUERY, LOCATION_DIM_QUERY, EVENT_DIM_QUERY, TARGET_DIM_QUERY, GROUP_DIM_QUERY, FACT_QUERY

from tqdm import tqdm

import time

import csv
import os

DIMS = {
    "group_dim": {
        "id": ["group_name"],
        "fields": ["group_name"],
        "type": "Group",
        "relation_name": "GROUP_FACT",
        "target_type": "Fact",
        "name": "group_name",
        "query": GROUP_DIM_QUERY
    },

    "location_dim": {
        "id": ["region", "country", "provstate", "city"],
        "fields": ["longitude", "latitude", "region", "country", "provstate", "city"],
        "type": "Location",
        "relation_name": "LOCATION_FACT",
        "target_type": "Fact",
        "name": "country",
        "query": LOCATION_DIM_QUERY
    },

    "time_dim": {
        "id": ["year", "month", "day"],
        "fields": ["year", "month", "day"],
        "type": "Time",
        "target_type": "Fact",
        "name": "year",
        "query": TIME_DIM_QUERY
    },

    "target_dim": {
        "id": "target",
        "fields": ["target", "target_nat", "target_entity", "target_type"],
        "type": "Target",
        "target_type": "Fact",
        "name": "target",
        "query": TARGET_DIM_QUERY
    },

    "event_dim": {
        "id": "event_id",
        "fields": ["event_id", "attack_type", "success", "suicide", "weapon_type", "individual", "nperps", "nperps_cap", "host_kid", "nhost_kid", "host_kid_hours", "host_kid_days", "ransom", "ransom_amt", "ransom_amt_paid", "total_killed", "perps_killed", "total_wounded", "perps_wounded", "property_dmg", "property_dmg_value"],
        "type": "Event",
        "target_type": "Fact",
        "name": "event_id",
        "query": EVENT_DIM_QUERY
    },

    "fact": {
    "fields": ["event_id", "group_name", "year", "month", "day", "region", "country", "provstate", "city", "target", "total_killed"],
    "type": "Fact",
    "name": "event_id",
    "query": FACT_QUERY
    }
}


DIM_RELATIONS = {
    "group_fact": {
        "relation_name": "GROUP_FACT",
        "source_type": "Group",
        "target_type": "Fact",
        "source_attributes": ["group_name"],
        "target_attributes": ["group_name"]
    },
    "location_fact": {
        "relation_name": "LOCATION_FACT",
        "source_type": "Location",
        "target_type": "Fact",
        "source_attributes": ["region", "country", "provstate", "city"],
        "target_attributes": ["region", "country", "provstate", "city"]
    },
    "time_fact": {
        "relation_name": "TIME_FACT",
        "source_type": "Time",
        "target_type": "Fact",
        "source_attributes": ["year", "month", "day"],
        "target_attributes": ["year", "month", "day"]
    },
    "target_fact": {
        "relation_name": "TARGET_FACT",
        "source_type": "Target",
        "target_type": "Fact",
        "source_attributes": ["target"],
        "target_attributes": ["target"]
    },
    "event_fact": {
        "relation_name": "EVENT_FACT",
        "source_type": "Event",
        "target_type": "Fact",
        "source_attributes": ["event_id"],
        "target_attributes": ["event_id"]
    }
}



class Graph:
    def __init__(self, uri) -> None:
        self.driver = GraphDatabase.driver(uri)
    
    def close_driver(self):
        self.driver.close()

    @staticmethod
    def _query(tx, query: str):
        
        res = tx.run(query)

        return res.data()

    def connect_nodes(self, query):
        with self.driver.session() as session:
            res = session.write_transaction(
                self._connect_nodes,
                query
            )

        return res
    
    def write_query(self, query):
        with self.driver.session() as session:
            res = session.write_transaction(
                self._query,
                query
            )
        
        return res

    
"""
LOAD CSV WITH HEADERS FROM 'file:///group_dim.csv' AS line
CREATE (:Group {name: line.group_name})
"""
    


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
        df.to_csv(f"graph_data_2/{key}.csv")
    end = time.time()

    print("time taken:", end - start)

    os.system("sudo cp graph_data_2/* graph_data/")

    g.write_query("MATCH (n) DETACH DELETE n")


    # CREATE NODES
    for key in DIMS.keys():
        dim = DIMS[key]
        create_query = {field: 'line.'+field for field in dim["fields"]}
        create_query["id"] = "line."+dim["name"]
        create_query = f"CREATE (:{dim['type']} {create_query}".replace("'", "")
        print(create_query)
        query = f"""LOAD CSV WITH HEADERS FROM 'file:///{key}.csv' AS line {create_query})"""
        print(query)
        g.write_query(query)
    
    # CREATE RELATIONSHIPS

    for key in DIM_RELATIONS.keys():
        relation_ship = DIM_RELATIONS[key]

        source = relation_ship["source_type"]
        target = relation_ship["target_type"]

        source_att = list(relation_ship["source_attributes"])
        target_att = relation_ship["target_attributes"]

        source_df = pd.read_csv("graph_data/" + source.lower() + "_dim.csv")
        target_df = pd.read_csv("graph_data/" + target.lower() + ".csv")

        source_att_df = source_df[source_att]

        source_att_df.to_csv("graph_data_2/relationships_" + source.lower() + "_" + target.lower() + ".csv")

        os.system("sudo cp graph_data_2/* graph_data/")

        query = f"""LOAD CSV WITH HEADERS FROM 'file:///relationships_{source.lower()}_{target.lower()}.csv' AS row MATCH """

        source_fields = {field: 'row.'+field for field in source_att}
        target_fields = {field: 'row.'+field for field in target_att}
        match_query = f"(x1:{source} {source_fields}), (x2:{target} {target_fields}) ".replace("'", "")

        query += match_query

        create_query = f"CREATE (x1)-[:{relation_ship['relation_name']}]->(x2);"

        query += create_query

        print(query)

        g.write_query(query)
    """
    LOAD CSV WITH HEADERS FROM "file:///Relationships.csv" AS row
    //look up the two nodes we want to connect up
    MATCH (p1:owner {id:row.id_from}), (p2:pet {id:row.id_to})
    //now create a relationship between them
    CREATE (p1)-[:OWNS]->(p2);
    """
