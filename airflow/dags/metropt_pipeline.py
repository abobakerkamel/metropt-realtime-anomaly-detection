from __future__ import annotations

import os
import socket
from datetime import datetime

import psycopg2
from airflow.sdk import DAG, task


def tcp_check(host: str, port: int, timeout: int = 5):
    with socket.create_connection((host, port), timeout=timeout):
        return True


with DAG(
    dag_id="metropt_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["metropt", "health-check"],
) as dag:

    @task
    def start_pipeline():
        return "starting checks"

    @task
    def check_postgres():
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=5432,
            dbname="metropt",
            user="metropt",
            password="metropt",
        )
        conn.close()
        return True

    @task
    def check_kafka():
        return tcp_check("broker", 19092)

    @task
    def check_spark():
        return tcp_check("spark-master", 7077)

    @task
    def finish_pipeline():
        return "all core services are reachable"

    start = start_pipeline()
    pg = check_postgres()
    kafka = check_kafka()
    spark = check_spark()
    finish = finish_pipeline()

    start >> [pg, kafka, spark] >> finish
