"""
import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class ImportBranchingOperator(BaseOperator):
    @apply_defaults
    def __init__(self, condition, task_id_false, task_id_true, *args, **kwargs):
        super(ImportBranchingOperator, self).__init__(*args, **kwargs)
        self.condition = condition
        self.task_id_false = task_id_false
        self.task_id_true = task_id_true


    @staticmethod
    def choose_branch():
        data_path = "/opt/airflow/dags/output/employees.csv"
        if os.path.exists(data_path):
            return "append_data_employees"
        else:
            return "get_data_employees"

    def execute(self, context):
        if self.condition:
            return self.task_id_true
        else:
            return self.task_id_false

    """