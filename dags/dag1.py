import datetime
import os

import pendulum
from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator
from operators.cleanDatabaseOperator import CleanDatabaseOperator
from operators.InsertDataOperator import InsertDataOperator
from utils.choose_branch import choose_branch
from operators.AppendDataOperator import AppendDataOperator
from operators.FetchDataOperator import FetchDataOperator
from operators.MergeDataOperator import MergeDataOperator
from operators.CleanDataOperator import CleanDataOperator
from operators.GetDataOperator import GetDataOperator
from operators.CreateTableOperator import CreateTableOperator
from dagfactory import load_yaml_dags
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
    CreateEmployeesTable = CreateTableOperator(
        task_id="create_employees_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="CREATE TABLE  employees(Serial_Number NUMERIC PRIMARY KEY,Company_Name TEXT,Employee_Markme TEXT,"
            "Description TEXT,Leave INTEGER);",

    )

    CreateEmployeesTempTable = CreateTableOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="CREATE TABLE  employees_temp(Serial_Number NUMERIC PRIMARY KEY,Company_Name TEXT,Employee_Markme TEXT,"
            "Description TEXT,Leave INTEGER);",

    )

    BranchTask = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch,
        provide_context=True,
        trigger_rule='none_failed',
    )

    GetDataEmployees = GetDataOperator(
        task_id="get_data_employees",
    )
    InsertDataEmployees = InsertDataOperator(
        task_id="insert_data_employees",
        postgres_conn_id="tutorial_pg_conn",
        trigger_rule='one_success'
    )
    AppendDataEmployees = AppendDataOperator(
        task_id="append_data_employees",
        postgres_conn_id="tutorial_pg_conn",

    )
    DataCleaning = CleanDataOperator(
        task_id="cleaning_employees",
        postgres_conn_id="tutorial_pg_conn",
        trigger_rule='none_failed',
    )
    DatabaseCleaning = CleanDatabaseOperator(
        task_id="cleaning_employees_temp",
        postgres_conn_id="tutorial_pg_conn",
        sql="UPDATE employees_temp SET Company_Name = UPPER(REPLACE(Company_Name, ',', '')), Employee_Markme = UPPER("
            "REPLACE(Employee_Markme, ',', '')),"
            "Description = UPPER(REPLACE(Description, ',', ''));",
        trigger_rule='none_failed',
    )

    MergeEmployeesTemp = MergeDataOperator(
        task_id="merge_employees_temp",
        postgres_conn_id="tutorial_pg_conn",
        sql="START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ; INSERT INTO employees SELECT * FROM employees_temp; "
            "COMMIT;",
        trigger_rule='none_failed',

    )

    FetchData = FetchDataOperator(
        task_id="fetch_employees_data",
        postgres_conn_id="tutorial_pg_conn",
        sql="SELECT * FROM employees",
        trigger_rule='none_failed'
    )

    """
        SET DEPENDENCIES
    """

    [CreateEmployeesTable,CreateEmployeesTempTable] >> BranchTask >> [GetDataEmployees,AppendDataEmployees] >> InsertDataEmployees
    InsertDataEmployees >> DatabaseCleaning >> DataCleaning >> MergeEmployeesTemp >> FetchData

dag = ProcessEmployees()
"""dag_factory = dagfactory.DagFactory("/opt/airflow/dags/other.yaml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())"""


load_yaml_dags(globals_dict=globals(), suffix=['other.yaml'])
