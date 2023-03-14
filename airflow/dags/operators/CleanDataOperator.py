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


class CleanDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, *args, **kwargs):
        super(CleanDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        try:

            # clean data in the csv file
            data_path = "/opt/airflow/dags/files/employees.csv"
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute("SELECT * FROM employees_temp")
            fieldnames = ['Serial Number', 'Company Name', 'Employee Markme', 'Description', 'Leave']
            with open(data_path, 'w') as file:
                writer = csv.writer(file)
                # Write the header row
                writer.writerow(fieldnames)

                # Loop over the rows and write them to the CSV file
                for row in cur:
                    writer.writerow(row)

            cur.close()
            conn.close()
        except Exception:
            raise CleanDataException("Not identified exception")
