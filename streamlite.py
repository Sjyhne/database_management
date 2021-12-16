import pandas as pd
import streamlit as st
import psycopg
import traceback

if __name__ == '__main__':

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
                
                cur.execute(""" SELECT sum(E.total_killed), T.year
                                FROM dwh.fact F, dwh.event_dim E, dwh.time_dim T
                                WHERE F.event_id = E.event_id and F.year = T.year and F.month = T.month and F.day = T.day
                                GROUP by T.year
                                ORDER by T.year 
                                """)
                
                st.title("Streamlit 101: An in-depth introduction")
                st.markdown("Welcome to this in-depth introduction to [...].")

                df = pd.DataFrame(cur.fetchall())

                print(df)
                
                st.dataframe(df.head())
                st.bar_chart(df)

                #close the communication with the PostgreSQL
                cur.close()
                conn.commit()

        except (Exception, psycopg.DatabaseError) as error:
                print("Error: ", error)
                traceback.print_exc()
        finally:
                if conn is not None:
                        conn.close()
                        
                print('Database connection with dwh is closed.')