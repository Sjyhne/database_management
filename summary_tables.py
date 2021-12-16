def load_summary_tables():
        
        commands = (
                """ 
                INSERT INTO summary.group_activity (
                    SELECT 
                    FROM dwh.fact F, dwh.date,
                )
                """
                ,
                """
                INSERT INTO dwh.location_dim (
                    {LOCATION_DIM_QUERY}
                ) ON CONFLICT DO NOTHING
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
        CREATE TABLE total_damage (
            target_type VARCHAR PRIMARY KEY
        )
        """
        ,
        """
        CREATE TABLE group_activity (
            group_activity_id SERIAL PRIMARY KEY,
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