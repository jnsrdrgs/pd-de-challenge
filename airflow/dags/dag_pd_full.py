from datetime import datetime
import pendulum
import airflow.utils.helpers as airflow_helpers
from airflow.models import DAG
from airflow.models import Variable

#from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from sql_queries import *
from db_utils import *
from airflow_operators_utils import *

# DAG Params Setup
SOURCE = "dag_passeidireto"
DW = "dw"
DAG_ID = f"passeidireto_etl.{SOURCE}"
local_timezone = pendulum.timezone("America/Sao_Paulo") #cron expressions in local time
START_DATE = datetime(2021, 1, 1, 0, 0, 0, tzinfo = local_timezone)

# Tables
COURSES = 'courses'
SESSIONS = 'sessions'
STUDENT_FOLLOW_SUBJECT = 'student_follow_subject'
STUDENTS = 'students'
SUBJECTS = 'subjects'
SUBSCRIPTIONS = 'subscriptions'
UNIVERSITIES = 'universities'

# S3 and other variables in Airflow
#ATHENA_QUERY_PATH = Variable.get("athena_result_location") (Fictional)
#ARTIFACTS_S3 = Variable.get("artifacts_s3_bucket")         (Fictional)
#DATALAKE_BUCKET = Variable.get("datalake_bucket")          (Fictional)
#S3_PREFIX = Variable.get("pd_s3_prefix")                   (Fictional)
#S3_BUCKET = Variable.get("s3_bucket")                      (Fictional)
#LOGS_PATH = f"s3://{S3_BUCKET}/logs/jobs/{DAG_ID}"         (Fictional)
#JOBS_PATH = f"{S3_PREFIX}/jobs/{SOURCE}/"                  (Fictional)



dag = DAG(
    dag_id=DAG_ID,
    default_args = {'owner': 'pd-data-team-candidate',
                    'depends_on_past': False,
                    'retries': 1,
                    'wait_for_downstream': False
    },
    description ='Create Dim Tables to Passei Direto DW',
    start_date = START_DATE)


# Task 1: Create schema:
create_dw = PythonOperator(
    task_id="create_dw",
    dag=dag,
    python_callable=create_database(DW),
)
    
# Task 2: Create tables at DW
create_tables = PythonOperator(
    task_id="create_tables",
    dag=dag,
    python_callable=create_tables(cur, conn),
)

# Task 3: Insert Dim Courses to DW
dim_courses_to_dw = PythonOperator(
    task_id='dim_courses_to_dw',
    dag=dag,
    python_callable=insert_to_dw(cur, conn, COURSES),
)

# Task 4: Insert Dim Sessions to DW
dim_sessions_to_dw = PythonOperator(
    task_id='dim_sessions_to_dw',
    dag=dag,
    python_callable=insert_to_dw(cur, conn, SESSIONS),
)

# Task 5: Insert Dim Subjects to DW
dim_following_subjects_to_dw = PythonOperator(
    task_id='dim_following_subjects_to_dw',
    dag=dag,
    python_callable=insert_to_dw(cur, conn, STUDENT_FOLLOW_SUBJECT),
)

# Task 6: Insert Dim Students to DW
dim_students_to_dw = PythonOperator(
    task_id='dim_students_to_dw',
    dag=dag,
    python_callable=insert_to_dw(cur, conn, STUDENTS),
)

# Task 7: Insert Dim Subjects to DW
dim_subjects_to_dw = PythonOperator(
    task_id='dim_subjects_to_dw',
    dag=dag,
    python_callable=insert_to_dw(cur, conn, SUBJECTS),
)

# Task 8: Insert Dim Subscriptions to DW
dim_subscriptions_to_dw = PythonOperator(
    task_id='dim_subscriptions_to_dw',
    dag=dag,
    python_callable=insert_to_dw(cur, conn, SUBSCRIPTIONS),
)

# Task 9: Insert Dim Universities to DW
dim_universities_to_dw = PythonOperator(
    task_id='dim_universities_to_dw',
    dag=dag,
    python_callable=insert_to_dw(cur, conn, UNIVERSITIES),
)

# Task 10: Create Dim Landing Page Views
dim_landing_page_views_to_dw = PostgresOperator(
    task_id='dim_landing_page_views_to_dw',
    dag=dag,
    python_callable=insert_partition_to_dw(cur, conn),
)

# Task 10: Create Dim User Marketing
#dim_user_marketing_to_dw = PostgresOperator(
#    task_id='dim_user_marketing_to_dw',
#    dag=dag,
#    postgres_conn_id="postgres",
#    sql=sql_queries.CREATE_DIM_USER_MARKETING,
#)

# Task 11: Data Validation
validate_data = PythonOperator(
    task_id='validate_data',
    dag=dag,
    python_callable=check_if_count_zero,
    provide_context=True,
    params={
        'table': 'dw.dim_landing_page_views',
    }
)


# Dependencies and lineage
create_dw >> create_tables >> [dim_courses_to_dw, dim_sessions_to_dw, dim_following_subjects_to_dw, 
                              dim_students_to_dw, dim_subscriptions_to_dw, dim_universities_to_dw,
                              dim_landing_page_views_to_dw] >> validate_data