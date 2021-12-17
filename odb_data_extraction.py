import psycopg

class SQL:
    def __init__(self) -> None:
        self.cur = self.create_connection_and_get_cursor()

    def create_connection_and_get_cursor(self):
        try:
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
            print("Connected")
            return cur
        except Exception as e:
            print("Could not connect:", e)

    def query_data(self, query):
        return [list(r) for r in list(self.cur.execute(query))]

    
class SQL_DWH:

    def __init__(self) -> None:
        self.cur = self.create_cur()
    
    def create_connection_and_get_cursor(self):
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