import os
import pandas as pd
from mongo_db import read_schema_jsons, insert_dict, connect_db, read_data
from graph import DIMS
from odb_data_extraction import SQL
import time
from tqdm import tqdm

def df_to_dict(df):
    arr = []
    for i in range(len(df.index)):
        arr.append(df.iloc[[i]].to_dict('records')[0])
    return arr

db = connect_db('mongodb://root:root@192.168.11.87:27017', 'terror_attacks')
dim_collections = ["group_dim", "location_dim", "date_dim", "target_dim", "event_dim"]

for dim in dim_collections:
    db[dim].drop()
    db.create_collection(dim)
db["fact"].drop()
db.create_collection("fact")


dim_url = "Collection_schemas/dim_schemas/"
for filename in os.listdir(dim_url):
    db.command(read_schema_jsons(dim_url, filename))


bolt_url = "bolt://localhost:7687"

sql = SQL()


dim_fields = DIMS["time_dim"]["fields"]
dim_query = DIMS["time_dim"]["query"]

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
    data.append(df.fillna(0))
#end = time.time()

#print("time taken:", end - start , "s")


data[4]['prop_dmg'].replace({0: "unknown"}, inplace=True)
print("Converting dataframes into dictionaries to prepare the data for insertion into mongodb")
for i, df in enumerate(data):
    d = df_to_dict(df)
    data[i] = d

print("Inserting all dictionaries into its respective collection")
for d, dim in zip(data, dim_collections):
    #print(d[0].keys())
    insert_dict(d, db[dim])

'''
insert_dict([date_dict], db["date_dim"])
insert_dict([location_dict], db["location_dim"])
insert_dict([event_dict], db["event_dim"])
insert_dict([target_dict], db["target_dim"])


ids = []
for coll in dim_collections:
    id = db[coll].find_one({}, {"_id":1})
    ids.append(id)

fact_dict = {"date_id": ids[0]['_id'], "loc_id": ids[1]['_id'], "event_id": ids[2]['_id'], "tar_id": ids[3]['_id']}
insert_dict([fact_dict], db["fact"])
'''