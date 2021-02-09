import os
import glob
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit
import psycopg2
from sql_queries import *

def insert_to_dw(cur, conn, table):
    """
    Clean the raw data and insert it to data warehouse.
    """
    
    path_file = os. getcwd() + f"/raw_data/BASE A/{table}.json"
    df = spark.read.json(path_file)

    if  table == 'student_follow_subject':
        df = df.select(concat("StudentId", lit('_'),"SubjectId"), "SubjectId", "StudentId", "FollowDate")
    elif table == 'sessions':
        df = df.select(concat("StudentId", lit('_'),"SessionStartTime"), "StudentId", "SessionStartTime")
    elif table == 'students':
        df = df.select("Id", "CourseId", "UniversityId", "StudentClient", "State", "City", "SignupSource", "RegisteredDate")
    elif table == 'subscriptions':
        df = df.select(concat("StudentId", lit('_'),"PaymentDate"), "StudentId", "PlanType", "PaymentDate")
    else:
        pass

    if table == 'courses':
        INSERT_TABLE = INSERT_COURSES_TABLE
    elif table == 'sessions':
        INSERT_TABLE = INSERT_SESSIONS_TABLE
    elif table == 'student_follow_subjects':
        INSERT_TABLE = INSERT_FOLLOW_SUBJECTS_TABLE
    elif table == 'students':
        INSERT_TABLE = INSERT_STUDENTS_TABLE
    elif table == 'subjects':
        INSERT_TABLE = INSERT_SUBJECTS_TABLE
    elif table == 'subscriptions':
        INSERT_TABLE = INSERT_SUBSCRIPTIONS_TABLE
    elif table == 'universities':
        INSERT_TABLE = INSERT_UNIVERSITIES_TABLE
    else:
        pass

    cur.execute(INSERT_TABLE, df)
    conn.commit()
    
def insert_partition_to_dw(cur, conn):
    """
    Clean the partition raw data and insert it to data warehouse.
    """

    path_file = os. getcwd() + "/raw_data/BASE B/*.json"
    df = spark.read.json(path_file)
    
    df = df.select("uuid", "session_uuid", "country", "region", "city_name",  "Page Category 1",\
                   "Page Category 2", "custom_1", "custom_2", "custom_4", "clv_total", "carrier",\
                   "language", "browser", "os_ver", "platform", "user_type", "device_new",\
                   "marketing_campaign", "marketing_medium", "marketing_source", "at")

    INSERT_LANDING_PAGE_VIEWS_TABLE
    cur.execute(INSERT_LANDING_PAGE_VIEWS_TABLE, df)
    conn.commit()
    
def check_if_count_zero(*args, **kwargs):
    """
    Validate if table in dw is not empty.
    """
    table = kwargs["params"]["table"]
    postgres_hook = PostgresHook("postgres")
    records = postgres_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data validation failed and {table} returned no results")
    num_records = records[0][0]
    n_records = records[0][0]
    if n_records < 1:
        raise ValueError(f"Data validation check failed and {table} contained 0 rows")
    logging.info(f"Data quality on table {table} was successful with {n_records[0][0]} records")
