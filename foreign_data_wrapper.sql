
CREATE EXTENSION postgres_fdw;

CREATE SERVER app_database_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', dbname 'gtd_dwh');

/*credentials that a user on the local server(odb) will use to make queries against the remote server(dwh)*/
CREATE USER MAPPING FOR CURRENT_USER
  SERVER app_database_server
  OPTIONS (user 'root', password 'root');

/*This will create foreign tables for all of the tables from our dwh schema into our reporting database’s odb schema.*/
CREATE SCHEMA dwh;
IMPORT FOREIGN SCHEMA dwh
  FROM SERVER app_database_server
  INTO dwh;