import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def create_database(database):
    """
    - Creates and connects to the database, returning connection and cursos to schema/database
    """
    
    # connect to default database:
    conn = psycopg2.connect("host=localhost dbname=postgres user=passeidireto password=passeidireto")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create datalake_raw schema:
    cur.execute("DROP DATABASE IF EXISTS " + database)
    cur.execute("CREATE DATABASE " + database + " WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to dw schema
    conn = psycopg2.connect("host=localhost dbname=" + {} + "user=candidate password=candidate")
    cur = conn.cursor()
    
    return cur, conn

def create_table(cur, conn):
    """
    Creates table using the queries in sql_queries file list. 
    """
    for query in create_table_queries:
            cur.execute(query)
            conn.commit()