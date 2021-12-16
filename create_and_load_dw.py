import psycopg
import pandas as pd
import traceback

#Connection to localhost with my static ip: http://192.168.183.101:5050

TIME_DIM_QUERY = """ 
                SELECT year, month, day
                FROM odb.date
                """

LOCATION_DIM_QUERY = """
                    SELECT ei.latitude, ei.longitude, co.region, p.country, c.provstate, ei.city, cp.population, cop.population
                    FROM odb.event_info AS ei
                        INNER JOIN
                        odb.city AS c
                        ON ei.city = c.city
                        INNER JOIN
                        odb.provstate AS p
                        ON c.provstate = p.provstate
                        INNER JOIN
                        odb.country AS co
                        ON p.country = co.country
                        INNER JOIN
                        odb.region AS r
                        ON co.region = r.region
                        INNER JOIN
                        odb.city_population AS cp
                        ON c.city = cp.city
                        INNER JOIN
                        odb.country_population AS cop
                        ON co.country = cop.country
                    """

TARGET_DIM_QUERY =  """
                    SELECT T.target, T.target_nat, T.entity_name, T.target_type
                    FROM odb.target AS t
                        INNER JOIN
                        odb.entity AS e
                        ON t.entity_name = e.entity_name
                        INNER JOIN
                        odb.target_type AS tt
                        ON t.target_type = tt.target_type
                    """

GROUP_DIM_QUERY =   """ 
                    SELECT group_name
                    FROM odb.attacker
                    """

EVENT_DIM_QUERY =   """
                    SELECT e.event_id, at.attack_type, ei.success, ei.suicide, wt.weapon_type, ei.individual, ei.nperps, ei.nperps_cap, ei.host_kid, 
                            ei.nhost_kid, ei.host_kid_hours, ei.host_kid_days, ei.ransom, ei.ransom_amt, ei.ransom_amt_paid, ei.total_killed, 
                            ei.perps_killed, ei.total_wounded, ei.perps_wounded, ei.property_dmg, ei.property_dmg_value
                    FROM odb.event_info AS ei
                        INNER JOIN
                        odb.attack_type AS at
                        ON ei.attack_type = at.attack_type
                        INNER JOIN
                        odb.weapon_type AS wt
                        ON ei.weapon_type = wt.weapon_type
                        INNER JOIN
                        odb.event AS e
                        ON ei.event_id = e.event_id
                    """
FACT_QUERY =        """
                    SELECT ei.event_id, at.group_name, d.year, d.month, d.day, co.region, p.country, c.provstate, ei.city, tt.target, sum(ei.total_killed), sum(ei.perps_killed), sum(ei.property_dmg_value)
                    FROM odb.event_info AS ei
                    INNER JOIN
                        odb.city AS c
                        ON ei.city = c.city
                        INNER JOIN
                        odb.provstate AS p
                        ON c.provstate = p.provstate
                        INNER JOIN
                        odb.country AS co
                        ON p.country = co.country
                        INNER JOIN
                        odb.region AS r
                        ON co.region = r.region
                        INNER join
                        odb.event as ev
                        ON ei.event_id = ev.event_id
                        INNER join
                        odb.attacker as at
                        ON ev.group_name = at.group_name
                        INNER join
                        odb.date as d
                        ON ei.year = d.year and ei.month = d.month and ei.day = d.day
                        INNER JOIN
                        odb.event AS ev
                        ON ei.event_id = ev.event_id
                        INNER JOIN
                        odb.target AS tt
                        ON ev.target = tt.target
                    GROUP BY ei.event_id, at.group_name, d.year, d.month, d.day, co.region, p.country, c.provstate, ei.city
                    """ 

FACT_QUERY = """
                SELECT ei.event_id, at.group_name, d.year, d.month, d.day, co.region, p.country, c.provstate, ei.city, sum(ei.total_killed)
                FROM odb.event_info AS ei
                INNER JOIN
                    odb.city AS c
                    ON ei.city = c.city
                    INNER JOIN
                    odb.provstate AS p
                    ON c.provstate = p.provstate
                    INNER JOIN
                    odb.country AS co
                    ON p.country = co.country
                    INNER JOIN
                    odb.region AS r
                    ON co.region = r.region
                    INNER join
                    odb.event as ev
                    ON ei.event_id = ev.event_id
                    INNER join
                    odb.attacker as at
                    ON ev.group_name = at.group_name
                    INNER join
                    odb.date as d
                    ON ei.year = d.year AND ei.month = d.month AND ei.day = d.day
                GROUP BY ei.event_id, at.group_name, d.year, d.month, d.day, co.region, p.country, c.provstate, ei.city
                """

def create_tables(cur):

        """ create tables in the PostgreSQL database"""

        #Drop schema if it already exist, create it and then change the search path
        cur.execute("DROP SCHEMA IF EXISTS dwh CASCADE; CREATE SCHEMA dwh; SET search_path TO dwh, public;")
        
        commands = (
                """ 
                CREATE TABLE time_dim (
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    day INTEGER NOT NULL,
                    PRIMARY KEY(year, month, day)
                )
                """
                ,
                """ 
                CREATE TABLE location_dim (   
                    latitude FLOAT,
                    longitude FLOAT,
                    region VARCHAR NOT NULL,
                    country VARCHAR NOT NULL,
                    provstate VARCHAR NOT NULL,
                    city VARCHAR NOT NULL,
                    PRIMARY KEY(region, country, provstate, city)
                )
                """
                ,
                """
                CREATE TABLE event_dim (
                    event_id VARCHAR PRIMARY KEY,
                    attack_type VARCHAR,
                    success INTEGER NOT NULL,
                    suicide INTEGER NOT NULL,
                    weapon_type VARCHAR NOT NULL,
                    individual INTEGER NOT NULL,
                    nperps INTEGER, 
                    nperps_cap INTEGER,
                    host_kid INTEGER NOT NULL,
                    nhost_kid INTEGER,
                    host_kid_hours INTEGER,
                    host_kid_days INTEGER,
                    ransom INTEGER NOT NULL,
                    ransom_amt INTEGER,
                    ransom_amt_paid INTEGER,
                    total_killed INTEGER,
                    perps_killed INTEGER,
                    total_wounded INTEGER,
                    perps_wounded INTEGER,
                    property_dmg INTEGER, 
                    property_dmg_value INTEGER,
                    city_population INTEGER,
                    country_population INTEGER
                )
                """  
                ,
                """
                CREATE TABLE target_dim (
                    target VARCHAR PRIMARY KEY,
                    target_nat VARCHAR,
                    target_entity VARCHAR,
                    target_type VARCHAR
                )
                """
                ,
                """
                CREATE TABLE group_dim (
                    group_name VARCHAR NOT NULL,      
                    PRIMARY KEY(group_name)
                )
                """
                ,
                """
                CREATE TABLE fact (
                    event_id VARCHAR NOT NULL,
                    group_name VARCHAR NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    day INTEGER NOT NULL,
                    region VARCHAR NOT NULL,
                    country VARCHAR NOT NULL,
                    provstate VARCHAR NOT NULL,
                    city VARCHAR NOT NULL,
                    total_killed INTEGER NOT NULL,
                    perps_killed INTEGER NOT NULL,
                    property_damage INTEGER NOT NULL,
                    target VARCHAR NOT NULL,
                    PRIMARY KEY (event_id, year, month, day, region, country, provstate, city),
                    FOREIGN KEY (event_id) REFERENCES event_dim(event_id),
                    FOREIGN KEY (group_name) REFERENCES group_dim(group_name),                        
                    FOREIGN KEY (year, month, day) REFERENCES time_dim(year, month, day),
                    FOREIGN KEY (region, country, provstate, city) REFERENCES location_dim(region, country, provstate, city),
                    FOREIGN KEY (target) target_dim(target)
                )
                """)
                        

        for command in commands:
                cur.execute(command)

def load_tables(cur):


        commands = (
                f""" 
                INSERT INTO dwh.time_dim (
                    {TIME_DIM_QUERY}
                )
                """
                ,
                f"""
                INSERT INTO dwh.location_dim (
                    {LOCATION_DIM_QUERY}
                ) ON CONFLICT DO NOTHING
                """
                ,
                f"""
                INSERT INTO dwh.target_dim (
                    {TARGET_DIM_QUERY}
                )
                """
                ,
                f""" 
                INSERT INTO dwh.group_dim (
                    {GROUP_DIM_QUERY}
                )
                """
                ,
                f"""
                INSERT INTO dwh.event_dim (
                    {EVENT_DIM_QUERY}
                )
                """
                ,
                f"""
                INSERT INTO dwh.fact (
                    {FACT_QUERY}
                )
                """)
        res = [r[2:] for r in list(cur.execute(LOCATION_DIM_QUERY))]

        print(res)
        for r in sorted(res):
            print(r)

        print(len(res))
        
        print(len(list(set(res))))

        for command in commands:
            print("command:", command)
            cur.execute(command)
                                   
        
    
def load_dwh_tables():
        conn = None
        try:
                # read connection parameters

                # connect to the PostgreSQL server
                print('Connecting to the odb PostgreSQL database to load the dwh...')
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
                
                #Create foreign tables that represent the tables in the data warehouse(Allows us to interact with the dwh from a single connection)
                print("EXECUTING FOREIGN DATA WRAPPER")
                cur.execute('''DROP EXTENSION IF EXISTS postgres_fdw CASCADE; DROP SCHEMA IF EXISTS dwh''')
                cur.execute(open("foreign_data_wrapper.sql", "r").read())
                print("EXECUTING FOREIGN DATA WRAPPER: ---- DONE")

                #Load data into dwh from odb
                load_tables(cur)
                
                
                #close the communication with the PostgreSQL
                cur.close()
                conn.commit()

        except (Exception, psycopg.DatabaseError) as error:
                print("Error: ", error)
        finally:
                if conn is not None:
                        conn.close()
                        
                print('Database connection with dwh is closed NOW!.')     

def create_dwh_tables():

        """ Connect to the PostgreSQL database server """
        conn = None
        try:
                # read connection parameters

                # connect to the PostgreSQL server
                print('Connecting to the dwh PostgreSQL database to create the tables...')
                conn = psycopg.connect(
                host="localhost",
                dbname = "gtd_dwh",
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
                
                #Create the tables for the star schema
                create_tables(cur)

               # close the communication with the PostgreSQL
                cur.close()
                conn.commit()

        except (Exception, psycopg.DatabaseError) as error:
                print("Error: ", error)
                traceback.print_exc()
        finally:
                if conn is not None:
                        conn.close()
                        
                print('Database connection with dwh is closed.')
        

if __name__ == '__main__':

        create_dwh_tables()
        
        load_dwh_tables()