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


class AppendDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, *args, **kwargs):
        super(AppendDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        try:
            data_path = "/opt/airflow/dags/files/employees.csv"
            url = Variable.get("url-data")
            # Send a HTTP request to the external URL and retrieve the response as a JSON object
            response = requests.get(url)
            new_rows = response.json()

            # Read the existing CSV file into a list of dictionaries
            with open(data_path, 'r') as file:
                reader = csv.DictReader(file)
                rows = [row for row in reader]

            # Check if each new row already exists in the list based on the ID field
            for new_row in new_rows:
                row_exists = False
                for row in rows:
                    if row['Serial Number'] == new_row['Serial Number']:
                        # Update the existing row with the new data
                        row.update(new_row)
                        row_exists = True
                        break
                if not row_exists:
                    # Add the new row to the end of the list
                    rows.append(new_row)

            # Write the updated list back to the CSV file
            with open(data_path, 'w', newline='') as file:
                writer = csv.DictWriter(file)
                writer.writerows(rows)

        except Exception:
            raise GetDataException("Not identified exception")

