import logging

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy_utils.types.pg_composite import psycopg2
from airflow.models import Variable
import os
import requests

from operators.exceptions.FetchDataException import FetchDataException


class FetchDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, sql, *args, **kwargs):
        super(FetchDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, context):
        try:
            logging.info("Task5 for fetching DATA is starting.")

            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()

            cur = conn.cursor()
            logging.info("cur exec SELECT")
            cur.execute(self.sql)
            logging.info("cur fetchall")
            result = cur.fetchall()
            for item in result:
                print(item)
            conn.commit()

            logging.info("Task5 completed successfully.")
        except Exception:
            raise FetchDataException("Not identified exception")
