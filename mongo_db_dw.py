import os
import pandas as pd
from mongo_db import read_schema_jsons, insert_dict, connect_db, read_data


def df_to_dict(df):
    arr = []
    for i in range(len(df.index)):
        arr.append(df.iloc[[i]].to_dict('records')[0])
    return arr

db = connect_db('mongodb://root:root@192.168.11.87:27017', 'terror_attacks')
dim_collections = ["date_dim", "location_dim", "event_dim", "target_dim"]

for dim in dim_collections:
    db[dim].drop()
    db.create_collection(dim)
db["fact"].drop()
db.create_collection("fact")


dim_url = "Collection_schemas/dim_schemas/"
for filename in os.listdir(dim_url):
    db.command(read_schema_jsons(dim_url, filename))

'''
data = read_data('test.csv')[0]
date_dict = {"year": int(data["year"]), "month": int(data["month"]), "day": int(data["day"])}
location_dict = {"latitude": float(data['latitude']), "longitude": float(data['longitude']), "region": data["region"], "country": data['country'], "provstate": data['provstate'], "city": data['city']}
event_dict = {"attack_type": data["attack_type"], "success": int(data["success"]), "suicide": int(data["suicide"]), "weapon_type": data["weapon_type"],
                "group_name": data["group_name"], "individual": int(data["individual"]), "nperps": int(data["nperps"]), "nperps_cap": int(data["nperps_cap"]),
                "host_kid": int(data['host_kid']), "nhost_kid": int(data['nhost_kid']), "host_kid_hours": int(data['host_kid_hours']),"host_kid_days": int(data['host_kid_days']),
                "ransom": int(data['ransom']),"ransom_amt": int(data['ransom_amt']),"ransom_amt_paid": int(data['ransom_amt_paid']), 
                "host_kid_outcome": data["host_kid_outcome"],"nreleased": int(data["nreleased"]),
                "total_killed": int(data['total_killed']), "perps_killed": int(data['perps_killed']), "total_wounded": int(data['total_wounded']),"perps_wounded": int(data['perps_wounded']),
                "property_dmg": int(data['property_dmg']),"property_dmg_value": int(data['property_dmg_value'])
                }
target_dict = {"target": data["target"], "target_nat": data["target_nat"], "target_entity": data["target_entity"], "target_type": data["target_type"]}

#test_df = pd.DataFrame({"year": [2020, 2021, 2019], "month": [5, 3, 7], "day": [13, 6, 17]})
#print(df_to_dict(test_df))

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