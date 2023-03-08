import collections
import csv
import json
import logging
from collections import OrderedDict

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

            data_path = "/opt/airflow/dags/output/employees.csv"
            url = Variable.get("url-data")
            # Send an HTTP request to the external URL and retrieve the response as a JSON object
            response = requests.get(url).text

            # get references :
            """serials = []
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            result = cur.execute("SELECT * FROM employees_temp;")
            for row in result:
                serials.append(row[0])"""

            # Extract a list of rows and each row is represented with list of its words

            lines = response.split("\n")
            new_rows = {"lines": []}
            for line in lines:
                words = line.split(',')
                new_rows["lines"].append(words)
            if len(new_rows["lines"]) > 0:
                new_rows["lines"].pop(0)

            # logging.info(f"new rows : {new_rows['lines']}")

            # Read the existing CSV file into a list of dictionaries
            with open(data_path, 'r') as file:
                reader = csv.reader(file)
                rows = [row for row in reader]
                rows.pop(0)
                # logging.info(f"rows : {rows}")

            # Check if each new row already exists in the list based on the ID field
            for new_row in new_rows["lines"]:
                if len(new_row) <= 1:
                    continue
                # logging.info(f"new_row : {new_row}")
                # logging.info(f"new_row[0] : {new_row[0]}")
                # logging.info(f"new_row[0] : {new_row[0]}")
                row_exists = False
                for row in rows:
                    # logging.info(f"row[0]: {row[0]}")
                    if row[0] == new_row[0]:
                        # Update the existing row with the new data
                        # logging.info(f"new_row : {new_row} ")
                        # logging.info(f"row.keys : {row.keys()} ")
                        # logging.info(f"row : {row} ")
                        # pos = 0
                        """for pos in range(len(row)):
                            # logging.info(f" values : {value} ")
                            row[pos] = new_row[pos]
                            row_exists = True"""
                        row[-1] = new_row[-1]
                        row_exists = True
                        break
                if not row_exists:
                    """not checked yet"""
                    # Add the new row to the end of the list
                    rows.append(new_row)

            # Write the updated list back to the CSV file
            #os.remove(data_path)

            logging.info(f"rows : {rows} ")
            fieldnames = ['Serial Number', 'Company Name', 'Employee Markme', 'Description', 'Leave']
            with open(data_path, 'w') as file:
                writer = csv.writer(file)
                writer.writerow(fieldnames)
                writer.writerows(rows)
                """for row in rows:
                    # logging.info(f"row to write after update : {row} ")
                    # logging.info(f"row type : {type(row)}")
                    input_row = []
                    for value in row:
                        # logging.info(f"zip result : {key, value} ")
                        input_row.append(value)
                    # logging.info(f"input row : {input_row}")
                    # logging.info(f"input row  type: {type(input_row)}")
                    if len(input_row) <= 1:
                        continue
                    writer.writerow(input_row)                 
                    
                """

        except Exception:
            raise GetDataException("Not identified exception")
