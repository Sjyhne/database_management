import psycopg
import pandas as pd
import traceback

def load_summary_tables(cur):
        
        commands = (
                """ 
                INSERT INTO summary.group_activity_summary (
                    SELECT T.year, G.group_name, count(F.event_id), sum(E.total_killed)
                    FROM dwh.fact F, dwh.event_dim E, dwh.group_dim G, dwh.time_dim T
                    WHERE F.event_id = E.event_id and F.group_name = G.group_name and F.year = T.year and F.month = T.month and F.day = T.day
                    GROUP by G.group_name, T.year
                    ORDER by T.year

                )
                """
                ,
                """ 
                INSERT INTO summary.country_damage_summary (
                    SELECT F.year, F.country, count(F.event_id), sum(E.total_killed), sum(E.property_dmg_value)
                    FROM dwh.fact F, dwh.event_dim E
                    WHERE F.event_id = E.event_id
                    GROUP by F.country, F.year
                    ORDER by F.year
                ) 
                """)
       
        for command in commands:
            print("command:", command)
            cur.execute(command)

def create_summary_tables(cur):

    #Drop schema if it already exist, create it and then change the search path
    cur.execute("DROP SCHEMA IF EXISTS summary CASCADE; CREATE SCHEMA summary; SET search_path TO summary, public;")

    commands = (
        """
        CREATE TABLE country_damage_summary (
            year VARCHAR,
            country VARCHAR,
            nattacks INTEGER,
            total_killed INTEGER,
            total_prop_dmg INTEGER
        )
        """
        ,
        """
        CREATE TABLE group_activity_summary (
            year VARCHAR,
            group_name VARCHAR,
            nattacks INTEGER,
            total_killed INTEGER
        )
        """)

    for command in commands:
        cur.execute(command)
        
        

if __name__ == '__main__':

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
                
                #Create and load the tables for the star schema
                create_summary_tables(cur)
                load_summary_tables(cur)

               # close the communication with the PostgreSQL
                cur.close()
                conn.commit()

        except (Exception, psycopg.DatabaseError) as error:
                print("Error: ", error)
        finally:
                if conn is not None:
                        conn.close()
                        
                print('Database connection with dwh is closed.')