import csv
import logging

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy_utils.types.pg_composite import psycopg2
from airflow.models import Variable
import os
import requests

from operators.exceptions.CleanDataException import CleanDataException


class CleanDatabaseOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, sql, *args, **kwargs):
        super(CleanDatabaseOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, context):
        try:
            # clean database data
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            result = cur.execute(self.sql)
            logging.info(f"number of rows affected : {result}")
            conn.commit()
            cur.close()
            conn.close()

        except Exception:
            raise CleanDataException("Not identified exception")
