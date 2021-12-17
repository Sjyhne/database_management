import pandas as pd
import streamlit as st
import psycopg
import time

class SQL_DWH:

    def __init__(self) -> None:
        self.cur = self.create_cur()
    
    def create_cur(self):
        try:
            print('Connecting to the PostgreSQL database...')
            conn = psycopg.connect(
            host="localhost",
            dbname = "gtd_dwh",
            user="root",
            password="root")

            #We set autocommit=True so every command we execute will produce
            conn.autocommit = True
            
            # create a cursor
            cur = conn.cursor()
            print("Connected")
            return cur
        except Exception as e:
            print("Could not connect:", e)

    def query_data(self, query):
        return [list(r) for r in list(self.cur.execute(query))]

def sql_queries(i):

        commands = [

                """ 
                SELECT sum(E.total_killed), T.year
                FROM dwh.fact F, dwh.event_dim E, dwh.time_dim T
                WHERE F.event_id = E.event_id and F.year = T.year and F.month = T.month and F.day = T.day
                GROUP by T.year
                ORDER by T.year 
                """
                ,
                 """ 
                SELECT T.target_nat, sum(E.ransom_amt), sum(E.ransom_amt_paid), SUM(F.total_killed)
                FROM dwh.event_dim E, dwh.fact F, dwh.target_dim T
                WHERE E.event_id = F.event_id and T.target = F.target
                GROUP BY T.target_nat
                HAVING (sum(E.ransom_amt) > 0) 
                """
                ,
                """ 
                SELECT F.city, E.city_population, COUNT(E.event_id)
                FROM dwh.event_dim E, dwh.fact F
                WHERE E.event_id = F.event_id
                GROUP BY F.city, E.city_population
                ORDER BY COUNT(E.event_id) DESC
                LIMIT 10

                """
                
        ]

        return commands[i]

if __name__ == '__main__':

        dw = SQL_DWH()
        
        st.title("The Global Terrorism Data Warehouse")
        st.markdown("By Jørgen Jacobsen, Sander Jyhne and Sigurd Jacobsen")

        st.header("Below we have run the same query on all the different warehouses and the operational database. ")
        

        st.subheader("PostgreSQL Data Warehouse")
        if st.button("RUN"):
                start = time.time()
                dw.cur.execute(sql_queries(1))
                end = time.time()
                res = end-start
                st.markdown(">Took {} to run".format(res))
                df = pd.DataFrame(dw.cur.fetchall())
                print(df)
                st.dataframe(df.head)
                st.bar_chart(df)
        st.subheader("Neo4j Warehouse")
        st.subheader("MongoDB Data Warehouse")
        st.subheader("MongoDB Data Warehouse")

        #df = pd.DataFrame(dw.cur.fetchall())

        #print(df)
        
        #st.dataframe(df.head())
        #st.bar_chart(df)

        #close the communication with the PostgreSQL
        dw.cur.close()
