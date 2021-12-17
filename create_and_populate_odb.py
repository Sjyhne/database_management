import psycopg
import pandas as pd 
import json
import traceback

from tqdm import tqdm

def extract_transform_populate_odb(cur):
 
    data = pd.read_csv('test.csv')

    gtd_columns = [{"table": "target_type", "pk": ["target_type"], "fields": ["target_type"]},
            {"table": "entity", "pk": ["target_entity"], "fields": ["target_entity"]},
            {"table": "attacker", "pk": ["group_name"], "fields": ["group_name"]},
            {"table": "region", "pk": ["region"], "fields": ["region"]},
            {"table": "country", "pk": ["country"], "fields": ["country", "region"]},
            {"table": "country", "pk": ["country"], "fields": ["target_nat"]},
            {"table": "provstate", "pk": ["provstate"], "fields": ["provstate", "country"]},
            {"table": "city", "pk": ["city"], "fields": ["city", "provstate"]},
            {"table": "attack_type", "pk": ["attack_type"], "fields": ["attack_type"]},
            {"table": "weapon_sub_type", "pk": ["weapon_sub_type"], "fields": ["weapon_sub_type"]},
            {"table": "weapon_type", "pk": ["weapon_type"], "fields": ["weapon_type", "weapon_sub_type"]},
            {"table": "target", "pk": ["target"], "fields": ["target", "target_nat", "target_type", "target_entity"]},
            {"table": "event", "pk": ["event_id"], "fields": ["event_id", "group_name", "target"]},
            {"table": "date", "pk": ["year", "month", "day"], "fields": ["year", "month", "day"]},
            {"table": "event_info", "pk": ["event_id"], "fields": ["event_id", "city", "year", "month", "day", "success", "suicide", "host_kid", "nhost_kid", "host_kid_hours", "host_kid_days", "ransom", "ransom_amt", "ransom_amt_paid", "longitude", "latitude", "nperps", "nperps_cap", "individual", "total_killed", "perps_killed", "total_wounded", "perps_wounded", "property_dmg", "property_dmg_value", "weapon_type", "attack_type", "country", "date"]}
            ]

    for index, i in tqdm(data.iterrows(), total=len(data.index)):
        
        for l in gtd_columns:
            table = l["table"]

            values = []
            pks = []
            query_values = ""

            if table != "fact":
                for col in l["fields"]:
                    if type(i[col]) != str:
                        values.append(str(i[col]))
                    else:
                        values.append(f"'{i[col]}'")

                for col in l["pk"]:
                    if type(i[col]) != str:
                        pks.append(str(col))
                    else:
                        pks.append(str(col))

                query_values += ", ".join(values)

            
                execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT DO NOTHING" % (table, query_values)
                if table == "country" and l["fields"][0] != "target_nat":
                    execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT (country) DO UPDATE SET region = %s" % (table, query_values, values[1])
                else:
                    execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT DO NOTHING" % (table, query_values)
                
                cur.execute(execstring)

    populate_db_populations(cur)


def populate_db_populations(cur):

    city_pop = pd.read_csv("etl_data/city_1970_1980.csv")
    country_pop = pd.read_csv("etl_data/country_1970_1980.csv")


    res = [r for r in cur.execute("SELECT * FROM country")]

    existing_countries = [r[0].strip("'") for r in cur.execute("SELECT country from country")]
    existing_cities = [r[0].strip("'") for r in cur.execute("SELECT city from city")]

    city_pop = city_pop.loc[city_pop["city"].isin(existing_cities)]
    country_pop = country_pop.loc[country_pop["country"].isin(existing_countries)]

    population_tables = [
        {"table": "country_population", "pk": ["year, country"], "fields": ["year", "country", "population"]},
        {"table": "city_population", "pk": ["year, country"], "fields": ["year", "city", "population"]}
    ]
    
    for index, i in tqdm(city_pop.iterrows(), total=city_pop.size):
        values = []
        city_dict = population_tables[1]
        table = city_dict["table"]
        for field in city_dict["fields"]:
            if type(i[field]) != str:
                values.append(str(i[field]))
            else:
                values.append(f"'{i[field]}'")
            
        query_values = ", ".join(values)
        execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT DO NOTHING" % (table, query_values)
        res = [r for r in cur.execute(f"SELECT city FROM city WHERE city = {values[1]}")]
        if len(res) > 0:
            cur.execute(execstring)
        else:
            ...
    

    for index, i in tqdm(country_pop.iterrows(), total=country_pop.size):
        values = []
        country_dict = population_tables[0]
        table = country_dict["table"]
        for field in country_dict["fields"]:
            if type(i[field]) != str:
                values.append(str(i[field]))
            else:
                values.append(f"'{i[field]}'")
            
        query_values = ", ".join(values)
        execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT DO NOTHING" % (table, query_values)
        res = [r for r in cur.execute(f"SELECT country FROM country WHERE country = {values[1]}")]
        if len(res) > 0:
            cur.execute(execstring)
        else:
            ...

                
 
def create_odb_tables(cur):
    """ create tables in the PostgreSQL database"""
 
    #Drop schema if it already exist, create it and then change the search path
    cur.execute("DROP SCHEMA IF EXISTS odb CASCADE; CREATE SCHEMA odb; SET search_path TO odb, public;")

    commands = (
        """
        CREATE TABLE target_type (
            target_type VARCHAR PRIMARY KEY
        )
        """
        ,
        """
        CREATE TABLE entity (
            entity_name VARCHAR PRIMARY KEY
        )
        """
        ,
        """
        CREATE TABLE attacker (
            group_name VARCHAR PRIMARY KEY
        )
        """
        ,
        """
        CREATE TABLE region (
            region VARCHAR PRIMARY KEY
        )
        """
        ,
        """
        CREATE TABLE country (
            country VARCHAR PRIMARY KEY,
            region VARCHAR,
            FOREIGN KEY (region) REFERENCES region(region)
        )
        """
        ,
        """
        CREATE TABLE provstate (
            provstate VARCHAR PRIMARY KEY,
            country VARCHAR,
            FOREIGN KEY (country) REFERENCES country(country)
        )
        """
        ,
        """
        CREATE TABLE city (
            city VARCHAR PRIMARY KEY,
            provstate VARCHAR,
            FOREIGN KEY (provstate) REFERENCES provstate(provstate)
        )
        """
        ,
        """    
        CREATE TABLE attack_type (
            attack_type VARCHAR PRIMARY KEY
        )
        """
        ,
        """
        CREATE TABLE weapon_sub_type (
            weapon_sub_type VARCHAR PRIMARY KEY
        )
        """
        ,
        """
        CREATE TABLE weapon_type (
            weapon_type VARCHAR PRIMARY KEY,
            weapon_sub_type VARCHAR,
            FOREIGN KEY (weapon_sub_type) REFERENCES weapon_sub_type(weapon_sub_type)
        )
        """
        ,
        """
        CREATE TABLE target (
            target VARCHAR PRIMARY KEY,
            target_nat VARCHAR,
            target_type VARCHAR,
            entity_name VARCHAR,
            FOREIGN KEY (target_nat) REFERENCES country(country),
            FOREIGN KEY (target_type) REFERENCES target_type(target_type),
            FOREIGN KEY (entity_name) REFERENCES entity(entity_name)
        )
        """
        ,
        """
        CREATE TABLE event (
            event_id VARCHAR PRIMARY KEY,
            group_name VARCHAR,
            target VARCHAR,
            FOREIGN KEY (group_name) REFERENCES attacker(group_name),
            FOREIGN KEY (target) REFERENCES target(target)
        )
        """
        ,
        """
        CREATE TABLE date (
            year INTEGER,
            month INTEGER,
            day INTEGER,
            PRIMARY KEY(year, month, day)
        )
        """
        ,
        """
        CREATE TABLE city_population(
            year INTEGER,
            city VARCHAR PRIMARY KEY,
            population INTEGER,
            FOREIGN KEY (city) REFERENCES city(city)
        )
        """
        ,
        """
        CREATE TABLE country_population(
            year INTEGER,
            country VARCHAR PRIMARY KEY,
            population INTEGER,
            FOREIGN KEY (country) REFERENCES country(country)
        )
        """
        ,
        """
        CREATE TABLE event_info (
            event_id VARCHAR PRIMARY KEY,
            city VARCHAR,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            success INTEGER,
            suicide INTEGER,
            host_kid INTEGER,
            nhost_kid INTEGER,
            host_kid_hours INTEGER,
            host_kid_days INTEGER,
            ransom INTEGER,
            ransom_amt INTEGER,
            ransom_amt_paid INTEGER,
            longitude FLOAT,
            latitude FLOAT,
            nperps INTEGER,
            nperps_cap INTEGER,
            individual INTEGER,
            total_killed INTEGER,
            perps_killed INTEGER,
            total_wounded INTEGER,
            perps_wounded INTEGER,
            property_dmg INTEGER,
            property_dmg_value INTEGER,
            weapon_type VARCHAR,
            attack_type VARCHAR,
            country VARCHAR,
            date DATE,
            FOREIGN KEY (event_id) REFERENCES event(event_id),
            FOREIGN KEY (city) REFERENCES city(city),
            FOREIGN KEY (weapon_type) REFERENCES weapon_type(weapon_type),
            FOREIGN KEY (attack_type) REFERENCES attack_type(attack_type),
            FOREIGN KEY (year, month, day) REFERENCES date(year, month, day)
        );
        """
        )

    
    for command in commands:
            cur.execute(command)

 
if __name__ == '__main__':

        """ Connect to the PostgreSQL database server """

        conn = None
        try:
                # read connection parameters
                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                conn = psycopg.connect(
                host="localhost",
                dbname = "gtd_odb",
                user="root",
                password="root")
 
                #We set autocommit=True so every command we execute will produce
                conn.autocommit = True
                
                # create a cursor
                cur = conn.cursor()
 
                # execute a statement
                print('PostgreSQL database version:')
                cur.execute('SELECT version()')
 
                # display the PostgreSQL database server version
                db_version = cur.fetchone()
                print(db_version)

                #Create odb tables
                create_odb_tables(cur)
            
                #populate odb
                extract_transform_populate_odb(cur)

               # close the communication with the PostgreSQL
                cur.close()
 
        except (Exception, psycopg.DatabaseError) as error:
                print("Error: ", error)
                traceback.print_exc()
        finally:
                if conn is not None:
                        conn.close()
 
                print('Database connection closed.')
        