# Data Engineering Test

Candidate: Jonas Inacio

## Table of contents

- [Attention](#attention)
    - [Requirements](#requirements)
- [Firing up your Database and Orchestrator](#firing-up-your-database-and-orchestrator)
- [The Project](#the-project)
    - [1. Cleaning the data and creating a dimension table](#1-cleaning-the-data-and-a-dimension-table)
    - [2. Using Airflow](#2-using-airflow)
- [Little Feedback](#little-feedback)

## Attention

Fork this repository in order to start building your solution. Then when you're done, send us a link to your repository with the solution.

## Requirements

- Docker version >= 19.03
- Docker-compose version >= 1.25
- Apache Airfloe version >= 1.10.10

Instruction on how to install [docker](https://docs.docker.com/get-docker/), [docker-compose](https://docs.docker.com/compose/install/) and [apache-airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html).


## Firing up your Database and Orchestrator

Fire up your PostgreSQL and Apache Airflow (Scheduler & Web UI Console) instance by opening a terminal and running the following command:

```bash
docker-compose up -d 
```

Using your prefered DB Client ([DBeaver](https://dbeaver.io/) for instace), you can connect to your PostgreSQL instance with the following credentials:

```
HOST: localhost
PASSWORD: 5342
USER: passeidireto
PASSWORD: passeidireto
DB: passeidireto
```

---
## The Project

The directory has a folder (`raw_data`) that contains:

- JSON data with PD business context;
- JSON partition data with page views. 

The idea is to use data engineering knowledge and skills to enrich the raw data and understand the relationship and create a better interface for analytics.


#### 1. Cleaning the data and creating a dimension table

As we have raw data, the possibility considered was to clean and enrich this data, making it available in a datalake warehouse (dw).

The JSON data was treated separately, identifying its context, characterizing as dimension tables in the model Star Schema [Kinball](https://docs.microsoft.com/en-us/power-bi/guidance/star-schema).

This way, after we have finished the dataframes, tables are created and then the data is inserted in Postgres db with this structure at DW:



**Table Dim Courses**
|	COLUMN		|TYPE			| CONSTRAINT	|
|---			|---			|---			|
|id				|BIGINT			|PRIMARY KEY	|
|name			|VARCHAR		|				|


**Table Dim Sessions**
|	COLUMN		|TYPE			| CONSTRAINT	|
|---			|---			|---			|
|id				|VARCHAR		|PRIMARY KEY	|
|id_student		|VARCHAR		|				|
|session_client	|VARCHAR		|				|
|ts_started		|TIMESTAMP		|				|


**Table Dim Following Subjects**
|	COLUMN		|TYPE			| CONSTRAINT	|
|---			|---			|---			|
|id				|VARCHAR		|PRIMARY KEY	|
|id_subject		|BIGINT			|				|
|id_student		|VARCHAR		|				|
|ts_following	|TIMESTAMP		|				|


**Table Dim Students**
|	COLUMN		|TYPE			| CONSTRAINT	|
|---			|---			|---			|
|id				|VARCHAR		|PRIMARY KEY	|
|id_course		|BIGINT			|				|
|id_university	|BIGINT			|				|
|student_type	|VARCHAR		|				|
|state			|VARCHAR		|				|
|city			|VARCHAR		|				|
|sign_up_source	|VARCHAR		|				|
|ts_registered	|TIMESTAMP		|				|


**Table Dim Subjects**
|	COLUMN		|TYPE			| CONSTRAINT	|
|---			|---			|---			|
|id				|BIGINT			|PRIMARY KEY	|
|name			|VARCHAR		|				|


**Table Dim Subscriptions**
|	COLUMN		|TYPE			| CONSTRAINT	|
|---			|---			|---			|
|id				|VARCHAR		|PRIMARY KEY	|
|id_student		|VARCHAR		|				|
|plan_type		|VARCHAR		|				|
|ts_payment		|TIMESTAMP		|				|


**Table Dim Universities**
|	COLUMN		|TYPE			| CONSTRAINT	|
|---			|---			|---			|
|id				|BIGINT			|PRIMARY KEY	|
|name			|VARCHAR		|				|


**Table Dim Landing Page Views**
|	COLUMN			|TYPE			| CONSTRAINT	|
|---				|---			|---			|
|id					|VARCHAR		|PRIMARY KEY	|
|id_user_session	|VARCHAR		|				|
|country			|VARCHAR		|				|
|region				|VARCHAR		|				|
|city				|VARCHAR		|				|
|page_main			|VARCHAR		|				|
|page_category		|VARCHAR		|				|
|custom_university	|VARCHAR		|				|
|custom_course		|VARCHAR		|				|
|custom_user		|VARCHAR		|				|
|clv_total			|SMALLINT		|				|
|carrier			|VARCHAR		|				|
|language			|VARCHAR		|				|
|browser			|VARCHAR		|				|
|os_version			|VARCHAR		|				|
|platform			|VARCHAR		|				|
|user_type			|VARCHAR		|				|
|device_new			|BOOLEAN		|				|
|mkt_campaign		|VARCHAR		|				|
|mkt_medium			|VARCHAR		|				|
|mkt_source			|VARCHAR		|				|
|ts_updated			|TIMESTAMP		|				|


#### 2. Using Airflow

If you already have the active images of the docker as previously reported (using command below)
```bash
docker-compose up -d 
```

You can start interacting with the Airflow UI and thus activate the DAG that will go through all the description done so far. Creating a schema to `DW` and cleaning all data to put tables there, where the `Dim` tables will be placed with data.

Waiting some seconds and you can access the airflow UI by: `localhost:8080`
There you will be able to start DAG and thus execute the execution of the entire data pipeline until the final objective of acquiring an environment with analytics dimensions.


### Little feedback

- Maybe it would be better to create a secret project and then be able to share it with some Passei Direto user, so there would be no problem with the code or the user being exposed to have this repository (in this case, I prefer to leave it in a secondary account on GitHub, to leave my main account intact).