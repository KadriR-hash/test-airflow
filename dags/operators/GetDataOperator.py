import csv
import logging

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy_utils.types.pg_composite import psycopg2
from airflow.models import Variable
import os
import requests


from operators.exceptions.GetDataException import GetDataException


class GetDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(GetDataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        try:

            logging.info("Task3 for getting DATA is starting.")
            data_path = "/opt/airflow/dags/output/employees.csv"
            url = Variable.get("url-data")

            os.makedirs(os.path.dirname(data_path), exist_ok=True)

            logging.info(f"getting DATA from  {url} is starting ")
            response = requests.request("GET", url)

            with open(data_path, "w") as file:
                file.write(response.text)
            logging.info("getting DATA from url completed successfully.")

            logging.info("Task3 completed successfully.")
        except psycopg2.errors.UniqueViolation:
            logging.info("Data duplicated")
            raise AirflowSkipException
        except Exception:
            raise GetDataException("Not identified exception")
