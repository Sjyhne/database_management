import psycopg
import pandas as pd
 
#Connection to localhost with my static ip: http://192.168.183.101:5050
 
def create_tables(cur):
 
        """ create tables in the PostgreSQL database"""
 
        #Drop tables if they exist
        cur.execute("DROP TABLE IF EXISTS time_dim, location_dim, attack_dim, target_dim, attacker_dim, hostage_dim, damage_dim, fact;")
 
        commands = (
                """ 
                CREATE TABLE time_dim (
                        time_id SERIAL PRIMARY KEY,                        
                        year INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        day INTEGER NOT NULL
                )
                """
                ,
                """ 
                CREATE TABLE location_dim (
                        loc_id SERIAL PRIMARY KEY,   
                        latitude FLOAT,
                        longitude FLOAT,
                        region VARCHAR NOT NULL,
                        country VARCHAR NOT NULL,
                        provstate VARCHAR NOT NULL,
                        city VARCHAR NOT NULL
                )
                """
                ,
                """
                CREATE TABLE attack_dim (
                        att_id SERIAL PRIMARY KEY,
                        attack_type VARCHAR NOT NULL,
                        success INTEGER NOT NULL,
                        suicide INTEGER NOT NULL,
                        weapon_type1 VARCHAR NOT NULL,
                        weapon_sub_type VARCHAR
                )
                """
                ,
                """
                CREATE TABLE target_dim (
                        tar_id SERIAL PRIMARY KEY,     
                        target VARCHAR NOT NULL,
                        target_nat VARCHAR,
                        target_entity VARCHAR,
                        target_type VARCHAR NOT NULL,
                        target_sub_type VARCHAR
 
                )
                """
                ,
                """
                CREATE TABLE attacker_dim (
                        attckr_id SERIAL PRIMARY KEY,     
                        group_name VARCHAR NOT NULL,
                        individual INTEGER NOT NULL,
                        nperps INTEGER, 
                        nperps_cap INTEGER
                )
                """
                ,
                """
                CREATE TABLE hostage_dim (
                        host_id SERIAL PRIMARY KEY,     
                        host_kid INTEGER NOT NULL,
                        nhost_kid INTEGER,
                        host_kid_hours INTEGER,
                        host_kid_days INTEGER,
                        ransom INTEGER NOT NULL,
                        ransom_amt INTEGER,
                        ransom_amt_paid INTEGER,
                        host_kid_outcome VARCHAR,
                        nreleased INTEGER
 
                        )
                """
                ,
                """
                CREATE TABLE damage_dim (
                        dmg_id SERIAL PRIMARY KEY,     
                        total_killed INTEGER,
                        perps_killed INTEGER,
                        total_wounded INTEGER,
                        perps_wounded INTEGER,
                        property_dmg INTEGER, 
                        property_dmg_value INTEGER,
                        prop_dmg VARCHAR
 
                )
                """
                ,
                """
                CREATE TABLE fact (
                        att_id INTEGER NOT NULL,
                        tar_id INTEGER NOT NULL,
                        attckr_id INTEGER NOT NULL,
                        host_id INTEGER NOT NULL,
                        dmg_id INTEGER NOT NULL,
                        time_id INTEGER NOT NULL,
                        loc_id INTEGER NOT NULL,
                        deaths INTEGER NOT NULL,
                        PRIMARY KEY (att_id, tar_id, attckr_id, host_id, dmg_id, time_id, loc_id),
                        FOREIGN KEY (att_id) REFERENCES attack_dim(att_id),
                        FOREIGN KEY (tar_id) REFERENCES target_dim(tar_id),
                        FOREIGN KEY (attckr_id) REFERENCES attacker_dim(attckr_id),
                        FOREIGN KEY (host_id) REFERENCES hostage_dim(host_id),
                        FOREIGN KEY (dmg_id) REFERENCES damage_dim(dmg_id),
                        FOREIGN KEY (time_id) REFERENCES time_dim(time_id),
                        FOREIGN KEY (loc_id) REFERENCES location_dim(loc_id)    
                )
                """)
 
 
        for command in commands:
                cur.execute(command)

import json

def extract_transform_load_data(cur):
 
        data = pd.read_csv('test.csv')
 
        #Rename table values
        #Split up date into three columns, date: 1977-05-01 -> year: 1977, month: 05, day: 01
        #data[["year", "month", "day"]] = data["date"].str.split("-", expand = True).astype(int)
        #data = data.drop('date', 1)
 
        #Set values to be integers instead of floats, 1.0 -> 1
        #pd.options.display.float_format = '{:,.0f}'.format

 
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
                {"table": "event", "pk": ["eventid"], "fields": ["eventid", "group_name", "target"]},
                {"table": "date", "pk": ["iyear", "imonth", "iday"], "fields": ["iyear", "imonth", "iday"]},
                {"table": "event_info", "pk": ["eventid"], "fields": ["eventid", "city", "iyear", "imonth", "iday", "success", "suicide", "host_kid", "nhost_kid", "host_kid_hours", "host_kid_days", "ransom", "ransom_amt", "ransom_amt_paid", "host_kid_outcome", "longitude", "latitude", "nperps", "nperps_cap", "individual", "total_killed", "perps_killed", "total_wounded", "perps_wounded", "property_dmg", "prop_dmg", "property_dmg_value", "weapon_type", "attack_type", "current_country", "date"]}
                ]
 
        for index, i in data.iterrows():
            print("INDEX:", index)
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

                    #Every dimension table uses auto incremented primary key, so we need to add default into the keys so they arent overwritten
                    #values.insert(0, "DEFAULT")

                    query_values += ", ".join(values)

                    #print(query_values)

                    #print("qvalues:", query_values)
                    #print("tables:", table)
                    #print(pks)
                    execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT DO NOTHING" % (table, query_values)
                    if table == "country" and l["fields"][0] != "target_nat":
                        execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT (country) DO UPDATE SET region = %s" % (table, query_values, values[1])
                    else:
                        execstring = "INSERT INTO %s VALUES (%s) ON CONFLICT DO NOTHING" % (table, query_values)
                    #for r in res:
                    #    print(r)
                    #print(execstring)
                    #print(execstring)
                    cur.execute(execstring)

 
        #cur.execute('''INSERT INTO fact(
        #                                        SELECT A.att_id, TA.tar_id, AT.attckr_id, H.host_id, D.dmg_id, T.time_id, L.loc_id, sum(D.total_killed)
        #                                        FROM time_dim T, location_dim L, attack_dim A, target_dim TA, attacker_dim AT, hostage_dim H, damage_dim D
        #                                        GROUP BY A.att_id, TA.tar_id, AT.attckr_id, H.host_id, D.dmg_id, T.time_id, L.loc_id);''')                                
 
def create_odb_tables(curr):
    """ create tables in the PostgreSQL database"""
 
    #Drop tables if they exist
    cur.execute("DROP TABLE IF EXISTS target_type, entity, attacker, region, country, provstate, city, attack_type, weapon_sub_type, weapon_type, target, event, event_info, date;")

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
            target_entity VARCHAR,
            FOREIGN KEY (target_nat) REFERENCES country(country),
            FOREIGN KEY (target_type) REFERENCES target_type(target_type),
            FOREIGN KEY (target_entity) REFERENCES entity(entity_name)
        )
        """
        ,
        """
        CREATE TABLE event (
            eventid VARCHAR PRIMARY KEY,
            group_name VARCHAR,
            target VARCHAR,
            FOREIGN KEY (group_name) REFERENCES attacker(group_name),
            FOREIGN KEY (target) REFERENCES target(target)
        )
        """
        ,
        """
        CREATE TABLE date (
            iyear INTEGER,
            imonth INTEGER,
            iday INTEGER,
            PRIMARY KEY(iyear, imonth, iday)
        )
        """
        ,
        """
        CREATE TABLE event_info (
            eventid VARCHAR PRIMARY KEY,
            city VARCHAR,
            iyear INTEGER,
            imonth INTEGER,
            iday INTEGER,
            success INTEGER,
            suicide INTEGER,
            host_kid INTEGER,
            nhost_kid INTEGER,
            host_kid_hours INTEGER,
            host_kid_days INTEGER,
            ransom INTEGER,
            ransom_amt INTEGER,
            ransom_amt_paid INTEGER,
            host_kid_outcome VARCHAR,
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
            prop_dmg VARCHAR,
            property_dmg_value INTEGER,
            weapon_type VARCHAR,
            attack_type VARCHAR,
            current_country VARCHAR,
            date DATE,
            FOREIGN KEY (eventid) REFERENCES event(eventid),
            FOREIGN KEY (city) REFERENCES city(city),
            FOREIGN KEY (weapon_type) REFERENCES weapon_type(weapon_type),
            FOREIGN KEY (attack_type) REFERENCES attack_type(attack_type),
            FOREIGN KEY (iyear, imonth, iday) REFERENCES date(iyear, imonth, iday)
        );
        """
        )


    for command in commands:
            cur.execute(command)

 
if __name__ == '__main__':

    with psycopg.connect(conninfo="host=localhost port=5432 dbname=test_db user=root password=root") as conn:
        print("Connected")
        with conn.cursor() as cur:
            # execute a statement
            print('PostgreSQL database version:')
            print(cur.execute('SELECT version()'))

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            create_odb_tables(cur)

            extract_transform_load_data(cur)

            tables = ["country", "attacker", "target", "event_info"]
            for table in tables:
                for r in cur.execute(f"SELECT * FROM {table}"):
                    print(r)            

        conn.commit()
            
        """
        conn = None
        try:
                # read connection parameters
 
                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                conn = psycopg.connect(
                host="localhost",
                database = "test_db",
                user="postgres",
                password="pw")
 
                #We set autocommit=True so every command we execute will produce
                conn.autocommit = True
 
                #Set isolation level to auto commit so that no transactions are started when commands are executed and no commit() or rollback() is required. Used in order to create db in psycopg.
                #conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
 
                # create a cursor
                cur = conn.cursor()
 
                # execute a statement
                print('PostgreSQL database version:')
                cur.execute('SELECT version()')
 
                # display the PostgreSQL database server version
                db_version = cur.fetchone()
                print(db_version)
 
                create_tables(cur)
 
                extract_transform_load_data(cur)
 
 
               # close the communication with the PostgreSQL
                cur.close()
                conn.commit()
 
        except (Exception, psycopg.DatabaseError) as error:
                print("Error: ", error)
        finally:
                if conn is not None:
                        conn.close()
 
                print('Database connection closed.')
        """