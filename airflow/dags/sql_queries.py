CREATE_DIM_COURSES_TABLE =  """
                                DROP TABLE IF EXISTS dw.dim_courses;
                                CREATE TABLE IF NOT EXISTS dw.dim_courses (
                                id BIGINT NOT NULL PRIMARY KEY,
                                name VARCHAR);                 
                            """


CREATE_DIM_SESSIONS_TABLE =  """
                                DROP TABLE IF EXISTS dw.dim_sessions;
                                CREATE TABLE IF NOT EXISTS dw.dim_sessions (
                                id VARCHAR NOT NULL PRIMARY KEY,
                                id_student VARCHAR,
                                session_client VARCHAR,
                                ts_started TIMESTAMP);                 
                             """

CREATE_DIM_FOLLOW_SUBJECTS_TABLE =   """
                                        DROP TABLE IF EXISTS dw.dim_following_subjects;
                                        CREATE TABLE IF NOT EXISTS dw.dim_following_subjects (
                                        id VARCHAR PRIMARY KEY,
                                        id_subject BIGINT,
                                        id_student VARCHAR,
                                        ts_following TIMESTAMP);
                                     """

CREATE_DIM_STUDENTS_TABLE =  """
                                DROP TABLE IF EXISTS dw.dim_students;
                                CREATE TABLE IF NOT EXISTS dw.dim_students (
                                id VARCHAR PRIMARY KEY,
                                id_course BIGINT,
                                id_university BIGINT,
                                student_type VARCHAR,
                                state VARCHAR,
                                city VARCHAR,
                                sign_up_source VARCHAR,
                                ts_registered TIMESTAMP);
                             """

CREATE_DIM_SUBJECTS_TABLE =  """
                                DROP TABLE IF EXISTS dw.dim_subjects;
                                CREATE TABLE IF NOT EXISTS dw.dim_subjects (
                                id BIGINT PRIMARY KEY,
                                name VARCHAR);
                             """

CREATE_DIM_SUBSCRIPTIONS_TABLE =  """
                                    DROP TABLE IF EXISTS dw.dim_subcriptions;
                                    CREATE TABLE IF NOT EXISTS dw.dim_subcriptions (
                                    id VARCHAR PRIMARY KEY,
                                    id_student VARCHAR,
                                    plan_type VARCHAR,
                                    ts_payment TIMESTAMP);
                                  """

CREATE_DIM_UNIVERSITIES_TABLE =  """
                                    DROP TABLE IF EXISTS dw.dim_universities;
                                    CREATE TABLE IF NOT EXISTS dw.dim_universities (
                                    id BIGINT PRIMARY KEY,
                                    name VARCHAR);
                                 """

CREATE_DIM_LANDING_PAGE_VIEWS_TABLE =    """
                                            DROP TABLE IF EXISTS dw.dim_landing_page_views;
                                            CREATE TABLE IF NOT EXISTS dw.dim_landing_page_views (
                                            id VARCHAR PRIMARY KEY,
                                            id_user_session VARCHAR,
                                            country VARCHAR,
                                            region VARCHAR,
                                            city VARCHAR,
                                            page_main VARCHAR,
                                            page_category VARCHAR,
                                            custom_university VARCHAR,
                                            custom_course VARCHAR,
                                            custom_user VARCHAR,
                                            clv_total SMALLINT,
                                            carrier VARCHAR,
                                            language VARCHAR,
                                            browser VARCHAR,
                                            os_version VARCHAR,
                                            platform VARCHAR,
                                            user_type VARCHAR,
                                            device_new BOOLEAN,
                                            mkt_campaign VARCHAR,
                                            mkt_medium VARCHAR,
                                            mkt_source VARCHAR,
                                            ts_updated TIMESTAMP);
                                         """

INSERT_COURSES_TABLE =        """
                                 INSERT INTO dw.dim_courses
                                 (id, name)
                                 VALUES (%s, %s) 
                              """

INSERT_SESSIONS_TABLE =        """
                                 INSERT INTO dw.dim_sessions
                                 (id, id_student, session_client, ts_started)
                                 VALUES (%s, %s, %s, %s) 
                               """

INSERT_FOLLOW_SUBJECTS_TABLE = """
                                 INSERT INTO dw.dim_following_subjects
                                 (id, id_subject, id_student, ts_following)
                                 VALUES (%s, %s, %s, %s) 
                               """

INSERT_STUDENTS_TABLE =      """
                                 INSERT INTO dw.dim_students
                                 (id, id_course, id_university, student_type, state, city, sign_up_source, ts_registered)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                             """

INSERT_SUBJECTS_TABLE =      """
                                 INSERT INTO dw.dim_subjects
                                 (id, name)
                                 VALUES (%s, %s) 
                             """

INSERT_SUBSCRIPTIONS_TABLE =   """
                                 INSERT INTO dw.dim_subcriptions
                                 (id, id_student, plan_type, ts_payment)
                                 VALUES (%s, %s, %s, %s) 
                               """

INSERT_UNIVERSITIES_TABLE =  """
                                 INSERT INTO dw.dim_universities
                                 (id, name)
                                 VALUES (%s, %s) 
                             """


INSERT_LANDING_PAGE_VIEWS_TABLE = """
                                 INSERT INTO dw.dim_landing_page_views
                                 (id, id_user_session, country, region, city, page_main, page_category, \
                                  custom_university, custom_course, custom_user, clv_total, carrier, \
                                  language, browser, os_version, platform, user_type, device_new, \
                                  mkt_campaign, mkt_medium, mkt_source, ts_updated)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                               """

create_table_queries = [CREATE_DIM_COURSES_TABLE, CREATE_DIM_SESSIONS_TABLE, CREATE_DIM_FOLLOW_SUBJECTS_TABLE,
                        CREATE_DIM_STUDENTS_TABLE, CREATE_DIM_SUBJECTS_TABLE, CREATE_DIM_SUBSCRIPTIONS_TABLE,
                        CREATE_DIM_UNIVERSITIES_TABLE, CREATE_DIM_LANDING_PAGE_VIEWS_TABLE]