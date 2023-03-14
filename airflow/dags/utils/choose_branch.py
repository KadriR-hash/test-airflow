import os



def choose_branch():

    data_path = "/opt/airflow/dags/output/employees.csv"
    if os.path.exists(data_path):
        return "append_data_employees"
    else:
        return "get_data_employees"
