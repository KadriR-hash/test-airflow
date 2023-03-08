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


class InsertDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, *args, **kwargs):
        super(InsertDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        try:

            data_path = "/opt/airflow/dags/output/employees.csv"
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur_insert = conn.cursor()
            logging.info("copying DATA from file to DB is starting")
            with open(data_path, "r") as file:

                """cur.copy_expert(
                    "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )"""

                reader = csv.reader(file)
                next(reader)  # skip the header row
                serials = []
                cur.execute("SELECT * FROM employees_temp;")
                for line in cur:
                    serials.append(line[0])

                for row in reader:
                    if row[0] in serials:
                        cur_insert.execute("INSERT INTO employees_temp (Serial_Number ,Company_Name,Employee_Markme,"
                                           "Description,Leave) VALUES (%s, %s, %s , %s ,%s) ;",
                                           row,
                                           )

            conn.commit()
            cur.close()
            cur_insert.close()
            conn.close()
            logging.info("copying DATA completed.")
        # except psycopg2.errors.UniqueViolation:
        #    logging.info("Data duplicated")
        #    raise AirflowSkipException
        except Exception:
            raise GetDataException("Not identified exception")
