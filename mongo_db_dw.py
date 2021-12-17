import os
import pandas as pd
from create_and_load_dw import FACT_QUERY, GROUP_DIM_QUERY, LOCATION_DIM_QUERY, TIME_DIM_QUERY, TARGET_DIM_QUERY, EVENT_DIM_QUERY
from mongo_db import read_schema_jsons, insert_dict, connect_db, read_data
from odb_data_extraction import SQL
import time
from tqdm import tqdm
import streamlit as st


import streamlit as st

DIMS = {
    "group_dim": {
        "fields": ["group_name"],
        "query": GROUP_DIM_QUERY
    },

    "location_dim": {
        "fields": ["latitude", "longitude", "region", "country", "provstate", "city"],
        "query": LOCATION_DIM_QUERY
    },

    "time_dim": {
        "fields": ["year", "month", "day"],
        "query": TIME_DIM_QUERY
    },

    "target_dim": {
        "fields": ["target", "target_nat", "target_entity", "target_type"],
        "query": TARGET_DIM_QUERY
    },

    "event_dim": {
        "fields": ["event_id", "attack_type", "success", "suicide", "weapon_type", "individual", "nperps", "nperps_cap", "host_kid", "nhost_kid", "host_kid_hours", "host_kid_days", "ransom", "ransom_amt", "ransom_amt_paid", "total_killed", "perps_killed", "total_wounded", "perps_wounded", "property_dmg", "property_dmg_value"],
        "query": EVENT_DIM_QUERY
    },
    "fact": {
        "fields": ["event_id", "group_name","target", "year","month","day","region","country","provstate","city","total_killed","perps_killed","property_damage"],
        "query": FACT_QUERY
    }
}
db = connect_db('mongodb://root:root@localhost:27017', 'terror_attacks')


def df_to_dict(df):
    arr = []
    for i in range(len(df.index)):
        arr.append(df.iloc[[i]].to_dict('records')[0])
    return arr

def main():
   
    dim_collections = ["group_dim", "location_dim", "date_dim", "target_dim", "event_dim", "fact"]

    for dim in dim_collections:
        db[dim].drop()
        db.create_collection(dim)
    db["fact"].drop()
    db.create_collection("fact")


    dim_url = "Collection_schemas/dim_schemas/"
    for filename in os.listdir(dim_url):
        db.command(read_schema_jsons(dim_url, filename))


    sql = SQL()

    #start = time.time()
    data = []
    print("Fetching data from SQL database and adding it to a dataframe")
    for key in DIMS.keys():
        dim_fields = DIMS[key]["fields"]
        dim_query = DIMS[key]["query"]
        df = pd.DataFrame(columns=dim_fields)
        for r in tqdm(sql.query_data(dim_query)):
            df = df.append(dict(zip(dim_fields, r)), ignore_index=True)
        if key == "location_dim":
            df = df.drop_duplicates()
        data.append(df)
    #end = time.time()
    #print("time taken:", end - start , "s")


    print("Converting dataframes into dictionaries to prepare the data for insertion into mongodb")
    for i, df in enumerate(data):
        d = df_to_dict(df)
        data[i] = d

    print("Inserting all dictionaries into its respective collection")
    for d, dim in zip(data, dim_collections):
        insert_dict(d, db[dim])


def attacks_killed_per_year_group():
    result = db['fact'].aggregate([
        {
            '$group': {
                '_id': {
                    'year': '$year', 
                    'group_name': '$group_name'
                }, 
                'num_attacks': {
                    '$sum': 1
                }, 
                'killed': {
                    '$sum': '$total_killed'
                }
            }
        }, {
            '$sort': {
                'killed': -1
            }
        }
    ])
    #for doc in result:
     #   print(doc)
    data = pd.DataFrame(result)
    return data

def attacks_killed_propdmg_per_year_group_country():
    result = db['fact'].aggregate([
        {
            '$group': {
                '_id': {
                    'year': '$year', 
                    'group_name': '$group_name',
                    'country': "$country"
                }, 
                'num_attacks': {
                    '$sum': 1
                }, 
                'killed': {
                    '$sum': '$total_killed'
                },
                "property_damage": {
                    "$sum": "$property_damage"
                }
            }
        }, {
            '$sort': {
                'property_damage': -1
            }
        }
    ])
    #for doc in result:
     #   print(doc)
    data = pd.DataFrame(result)
    return data

def data_for_map():
    result = db['location_dim'].find({},{"latitude": 1, "longitude": 1, "_id": 0})

    lats, longs = [], []
    for d in result:
        print(d)
        lats.append(d['latitude'])
        longs.append(d['longitude'])

    data = pd.DataFrame()

    data["latitude"] = lats
    data["longitude"] = longs
    return data

if __name__ == "__main__":
    #main()
    #print(attacks_killed_per_year_group())
    #print(attacks_killed_propdmg_per_year_group_country())
    data = data_for_map()

    st.caption("Map Showing Attacks")
    st.map(data)

    '''
if __name__ == "__main__":

    main()

    akpyg = attacks_killed_per_year_group()
    akppygc = attacks_killed_propdmg_per_year_group_country()

    h = akpyg["_id"].to_numpy()

    yrs = [l["year"] for l in h]
    groups = [l["group_name"] for l in h]

    print(h)

    df = pd.DataFrame(columns=["Group Name", "Year", "Nr of Attacks", "Total Killed"])
    df["Group Name"] = groups
    df["Year"] = yrs
    df["Nr of Attacks"] = akpyg["num_attacks"].to_numpy()
    df["Total Killed"] = akpyg["killed"].to_numpy()

    print(h)

    st.title("Global Terrorism Data")
    st.markdown("Information for counter-terrorism and travelling")

    st.dataframe(df)

'''
