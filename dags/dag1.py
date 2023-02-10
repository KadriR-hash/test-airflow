import datetime
import pendulum
import os
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.models import Variable


""" 
        process-employees DAG
"""


@dag(
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 2, 6, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
    try:
        logging.info("Task1 for creating employees table is starting.")
        create_employees_table = PostgresOperator(
            task_id="create_employees_table",
            postgres_conn_id="tutorial_pg_conn",
            sql="./sql/employees_schema.sql",
        )
        logging.info("Task1 completed successfully.")
    except Exception as e:
        return "failed to process Task1"

    try:
        logging.info("Task2 for creating employees emp table is starting.")
        create_employees_temp_table = PostgresOperator(
            task_id="create_employees_temp_table",
            postgres_conn_id="tutorial_pg_conn",
            sql="./sql/employees_temp.sql",
        )
        logging.info("Task2 completed successfully.")
    except Exception as e:
        return "failed to process Task2"

    try:
        logging.info("cleaning data")
        data_cleansing_temp = PostgresOperator(
            task_id="cleaning_employees_temp",
            postgres_conn_id="tutorial_pg_conn",
            sql="./sql/data_cleansing.sql",
            autocommit=True,
        )
        logging.info("cleaning data completed successfully.")
    except Exception as e:
        return "failed to process cleaning"

    try:
        logging.info("merging data")
        merge_employees_temp = PostgresOperator(
            task_id="fetch_employees_temp",
            postgres_conn_id="tutorial_pg_conn",
            sql="./sql/insert_into_employees.sql",
            autocommit=True,
        )
        logging.info("merging data completed successfully.")
    except Exception as e:
        return "failed to process merging"

    @task
    def get_data():
        try:

            logging.info("Task3 for getting DATA is starting.")
            data_path = "/opt/airflow/dags/files/employees.csv"
            url = Variable.get("url-data")

            os.makedirs(os.path.dirname(data_path), exist_ok=True)

            logging.info(f"getting DATA from  {url} is starting ")
            response = requests.request("GET", url)
            with open(data_path, "w") as file:
                file.write(response.text)
            logging.info("getting DATA from url completed successfully.")

            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            logging.info("copying DATA from file to DB is starting")
            with open(data_path, "r") as file:
                cur.copy_expert(
                    "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()
            logging.info("copying DATA completed.")

            logging.info("Task3 completed successfully.")
        except Exception as e:
            return "SOMETHING WENT WRONG ... COULDN'T RETRIEVE DATA"

    @task
    def fetch_data():
        # query = "./sql/select_employees_temp.sql"
        try:
            from utils import DecimalEncoder
            from json import JSONEncoder
            import json
            logging.info("Task5 for fetching DATA is starting.")

            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()

            cur = conn.cursor()
            logging.info("cur exec SELECT")
            cur.execute("SELECT * FROM employees")
            logging.info("cur fetchall")
            result = cur.fetchall()
            print(result)
            """
            logging.info("processing...")
            
            for row in result:
                for field in row:
                    tmp = json.dumps(field, cls=DecimalEncoder)
                    data_path ="/opt/airflow/dags/files/fetch.csv"
                    with open(data_path, "w") as file:
                        file.write()
            logging.info("finish processing !")
            """
            conn.commit()

            logging.info("Task5 completed successfully.")
        except Exception as e:
            return "SOMETHING WENT WRONG ... COULDN'T FETCH DATA"

    """
        SET DEPENDENCIES
    """

    [create_employees_table, create_employees_temp_table] >> get_data() >> data_cleansing_temp >> merge_employees_temp >> fetch_data()


dag = ProcessEmployees()
