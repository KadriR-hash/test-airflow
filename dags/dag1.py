import datetime
import os

import pendulum
from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label

from operators.AppendDataOperator import AppendDataOperator
from operators.FetchDataOperator import FetchDataOperator
from operators.MergeDataOperator import MergeDataOperator
from operators.CleanDataOperator import CleanDataOperator
from operators.GetDataOperator import GetDataOperator
from operators.CreateTableOperator import CreateTableOperator


def choose_branch(**kwargs):
    """
        Run an extra branch on if the file employees.csv exists and not empty !
        """
    data_path = "/opt/airflow/dags/files/employees.csv"
    if os.path.exists(data_path):
        return "append_data_employees"
    else:
        return "get_data_employees"


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
    create_employees_table = CreateTableOperator(
        task_id="create_employees_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="CREATE TABLE  employees(Serial_Number NUMERIC PRIMARY KEY,Company_Name TEXT,Employee_Markme TEXT,"
            "Description TEXT,Leave INTEGER);",

    )

    create_employees_temp_table = CreateTableOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="CREATE TABLE  employees_temp(Serial_Number NUMERIC PRIMARY KEY,Company_Name TEXT,Employee_Markme TEXT,"
            "Description TEXT,Leave INTEGER);",

    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=choose_branch
    )

    get_data_employees = GetDataOperator(
        task_id="get_data_employees",
        postgres_conn_id="tutorial_pg_conn",
        trigger_rule='none_failed',

    )
    append_data_employees = AppendDataOperator(
        task_id="append_data_employees",
        postgres_conn_id="tutorial_pg_conn",
        trigger_rule='none_failed',
    )
    data_cleansing_temp = CleanDataOperator(
        task_id="cleaning_employees_temp",
        postgres_conn_id="tutorial_pg_conn",
        sql="UPDATE employees_temp SET Company_Name = UPPER(Company_Name), Employee_Markme = UPPER(Employee_Markme),"
            "Description = UPPER(Description);",
        trigger_rule='none_failed'
    )

    merge_employees_temp = MergeDataOperator(
        task_id="merge_employees_temp",
        postgres_conn_id="tutorial_pg_conn",
        sql="START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ; INSERT INTO employees SELECT * FROM employees_temp; "
            "COMMIT;",
        trigger_rule='none_failed',

    )

    fetch_data = FetchDataOperator(
        task_id="fetch_employees_data",
        postgres_conn_id="tutorial_pg_conn",
        sql="SELECT * FROM employees",
        trigger_rule='none_failed'
    )

    """
        SET DEPENDENCIES
    """

    [create_employees_table,
     create_employees_temp_table] >> branch_task
    branch_task >> Label(
        "File not exists") >> get_data_employees >> data_cleansing_temp >> merge_employees_temp >> fetch_data
    branch_task >> Label(
        "File exists") >> append_data_employees >> data_cleansing_temp >> merge_employees_temp >> fetch_data


dag = ProcessEmployees()
