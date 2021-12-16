def load_summary_tables():
        
        commands = (
                """ 
                INSERT INTO summary.group_activity (
                    SELECT T.year, G.group_name, count(F.event_id), sum(E.total_killed)
                    FROM dwh.fact F, dwh.event_dim E, dwh.group_dim G, dwh.time_dim T
                    WHERE F.event_id = E.event_id and F.group_name = G.group_name and F.year = T.year and F.month = T.month and F.day = T.day
                    GROUP by G.group_name, T.year
                    ORDER by T.year

                )
                """
                ,
                """ Hent bare fra fact
                INSERT INTO summary.country_damage (
                    SELECT T.year, L.country, count(F.event_id), sum(E.total_killed), sum(E.property_dmg_value)
                    FROM dwh.fact F, dwh.event_dim E, dwh.group_dim G, dwh.location_dim L, dwh.time_dim T
                    WHERE F.event_id = E.event_id and F.group_name = G.group_name and F.region = L.region and 
                    F.provstate = L.provstate and F.country = L.country and F.city = L.city and
                    F.year = T.year and F.month = T.month and F.day = T.day
                    GROUP by G.group_name, L.country
                    ORDER by D.year
                ) 
                """
                ,
                """
                INSERT INTO dwh.target_dim (
                    {TARGET_DIM_QUERY}
                )
                """
                ,
                """ 
                INSERT INTO dwh.group_dim (
                    {GROUP_DIM_QUERY}
                )
                """
                ,
                """
                INSERT INTO dwh.event_dim (
                    {EVENT_DIM_QUERY}
                )
                """
                ,
                """
                INSERT INTO dwh.fact (
                    {FACT_QUERY}
                )
                """)
       
        for command in commands:
            print("command:", command)
            cur.execute(command)

def create_summary_tables():

    #Drop schema if it already exist, create it and then change the search path
    cur.execute("DROP SCHEMA IF EXISTS summary CASCADE; CREATE SCHEMA summary; SET search_path TO summary, public;")

    commands = (
        """
        CREATE TABLE country_damage (
            year VARCHAR,
            country VARCHAR,
            nattacks INTEGER,
            total_killed INTEGER,
            total_prop_dmg INTEGER
        )
        """
        ,
        """
        CREATE TABLE group_activity (
            year VARCHAR,
            group_name VARCHAR,
            nattacks INTEGER,
            total_killed INTEGER
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
        """)
        
        

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