#!/usr/bin/env bash
airflow initdb
airflow variables -i /pd-de-challenge/airflow/dags/local_env.json
airflow webserver