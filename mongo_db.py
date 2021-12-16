import csv
import sys
import json 
import os

from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from pprint import pprint
from collections import OrderedDict
from tqdm import tqdm

def read_data(pathname):
    with open(pathname, mode='r', encoding="utf8") as csv_file:
        lines = len(csv_file.readlines())

    data = []
    with open(pathname, mode='r', encoding="utf8") as infile:
        reader = csv.reader(infile)
        first_row = next(reader)
        for row in tqdm(reader, total=lines):
            if(row == first_row):
                continue
            dict = {}
            for i in range(len(row)):
                dict[first_row[i]] = row[i]
            data.append(dict)
            

    
    return data

#Creates a database connection
def connect_db(connection_url, db_name):
    client = MongoClient(connection_url)
    db = client[db_name]
    return db

#Applies the validations schemas to each collection
def read_schema_jsons(dim_url, filename):

    with open(os.path.join(dim_url, filename), 'r') as json_file:
        dict_from_file = json.loads(json_file.read())
    dict_from_file = OrderedDict(dict_from_file)
    return dict_from_file

    
#Tries to insert every dictionary from data into the correct collection in the database
def insert_dict(data, collection):
    for dict in tqdm(data, total=len(data)):
        try:
            collection.insert_one(dict)
        except:
            print("Insert failed: ", sys.exc_info())
            return False
    return True

def create_dicts(data):
    event_dicts = []
    target_dicts = []
    perpetrator_dicts = []

    for d in tqdm(data, total=len(data)):
        target_dict = {}
        event_dict = {}
        perpetrator_dict = {}

        target_dict['target_name'] = d['target']
        target_dict['target_type'] = d['target_type']
        target_dict['target_subtype'] = d['target_sub_type']
        target_dict['target_nationality'] = d['target_nat']

        #event_dict["attack_type"] = d[]
        event_dict['location'] = {"city": d['city'], "provstate": d['provstate'], "country": d['country_txt'], "region": d['region_txt']}
        event_dict['weapon_type'] = {"type": list(filter(None,[d['weaptype1_txt'], d['weaptype2_txt'], d['weaptype3_txt']])),
                                     "sub_type": list(filter(None,[d['weapsubtype1_txt'], d['weapsubtype2_txt'], d['weapsubtype3_txt']]))}
        event_dict['target_name'] = list(filter(None,[d['target1'], d['target2'], d["target3"]]))
        if(len(d['nkill']) > 0):
            event_dict["n_killed"] = int(float(d['nkill']))

        date = [int(n) for n in d['date'].split("-")]
        if(date[0] > 0 and date[1] > 0 and date[2] > 0):
            date = datetime(date[0], date[1], date[2])
            event_dict["date"] = date
        else:
            pass
            # event_dict["date"] = datetime.datetime(1, 1, 1)

        event_dict["position"] = {"lat": d['latitude'], "lon": d['longitude']}
        event_dict["perpetrator_name"] = list(filter(None,[d['gname'], d['gname2'], d["gname3"]]))
        event_dict["property_damage"] = {"is_known": d["property"], "extent": d["propextent_txt"], "value": d["propvalue"]}

        event_dict["ransom"] = {"ransom": d["ransom"], "ransom_demanded": d["ransomamt"], "ransom_paid": d["ransompaid"]}

        perpetrator_dict['name'] = list(filter(None,[d['gname'], d['gname2'], d["gname3"]]))
        perpetrator_dict['subname'] = list(filter(None, [d['gsubname'], d['gsubname2'], d["gsubname3"]]))
        perpetrator_dict['n_perps'] = d['nperps']
        perpetrator_dict ['n_perps_cap'] = d['nperpcap']

        target_dicts.append(target_dict)
        event_dicts.append(event_dict)
        perpetrator_dicts.append(perpetrator_dict)
    return target_dicts, event_dicts, perpetrator_dicts


def init_database(connection_url, db_name):

    db = connect_db(connection_url, db_name)
    #Checks if number of collctions equals 0, if its not, the database is already initialized and reading and inserting data is not necessary
    #if(len(db.list_collection_names()) < 3):
    db.event.drop()
    db.target.drop()
    db.perpetrator.drop()
    event = db.create_collection("event")
    target = db.create_collection("target")
    perpetrator = db.create_collection("perpetrator")

    for filename in os.listdir("Collection_schemas/"):
        db.command(read_schema_jsons(filename))

    data = read_data('test.csv')
    print("Data Read...")

    target_dicts, event_dicts, perpetrator_dicts = create_dicts(data)
    print("Dictionaries Created...")
    if(insert_dict(target_dicts, target)):
        print("Inserted Targets Successfully")
    if(insert_dict(event_dicts, event)):
        print("Inserted Events Successfully")
    if(insert_dict(perpetrator_dicts, perpetrator)):
        print("Inserted Perpetrators Successfully")
    print("Database Initialized")
    #else:
    #    print("Page reloaded")
    return db
if __name__ == "__main__":

    init_database('mongodb://192.168.11.87:27017', 'terror_attacks')

    