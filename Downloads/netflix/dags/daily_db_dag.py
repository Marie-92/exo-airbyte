import json
import logging
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}

def _transfer_daily_ratings_to_postgres():
    df = pd.read_csv("db_daily.csv")
    logging.info("CSV file transformed to dataframe:")
    logging.info(df.head())  
    postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
    conn = postgres_hook.get_sqlalchemy_engine()
    db_uri = postgres_hook.get_uri()        
    df.to_sql("daily_ratings", conn, if_exists="replace", index=False)
    logging.info(f"Dataframe successfully stored in database {db_uri} !")

with DAG(dag_id="netflix_daily_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    start = DummyOperator(task_id="start")

    create_daily_table = PostgresOperator(
        task_id="create_daily_table",
        sql="""
        CREATE TABLE IF NOT EXISTS daily_ratings (
            movie_id PRIMARY KEY,
            movie_title VARCHAR,
            user PRIMARY KEY,
            rating INTEGER,
            timestamp DATE,
            predicted_rating INTEGER
            )
            """,
        postgres_conn_id="postgres_default",
        )

    transfer_daily_ratings_to_postgres = PythonOperator(task_id="transfer_daily_ratings_to_postgres", python_callable=_transfer_daily_ratings_to_postgres)
    
    end = DummyOperator(task_id="end")

    start >> create_daily_table >> transfer_daily_ratings_to_postgres >> end
