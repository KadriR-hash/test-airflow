import logging

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy_utils.types.pg_composite import psycopg2
from airflow.models import Variable
import os
import requests

from operators.exceptions.MergeDataException import MergeDataException


class MergeDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, sql, *args, **kwargs):
        super(MergeDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, context):
        try:
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            result = cur.execute(self.sql)
            logging.info(f"number of rows affected : {result}")
            conn.commit()
            cur.close()
            conn.close()
        except psycopg2.errors.UniqueViolation:
            logging.info("Data duplicated")
            raise AirflowSkipException
        except Exception:
            raise MergeDataException("Not identified exception")
