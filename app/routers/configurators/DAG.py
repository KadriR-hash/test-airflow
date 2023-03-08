import logging
from abc import ABC
from fastapi import HTTPException
import requests
from fastapi.encoders import jsonable_encoder
from dotenv import load_dotenv
import os
from .DAGConfigurator import DAGConfigurator
from .models.SimpleDAGConfigurator import SimpleDAGConfigurator

logging.basicConfig(level=logging.INFO)


class DAG(DAGConfigurator, ABC):
    """def __init__(self, simple_dag_configurator):
        self.simple_dag_configurator = simple_dag_configurator
        self.tasks = []"""

    def create_dag(self, dag: SimpleDAGConfigurator):
        """load_dotenv()
        airflow_api_url = os.environ.get('AIRFLOW_API_URL')
        airflow_username = os.environ.get('AIRFLOW_USERNAME')
        airflow_password = os.environ.get('AIRFLOW_PASSWORD')
        payload = jsonable_encoder(dag)
        response = requests.post(f'{airflow_api_url}/dags',
                                 json=payload,
                                 auth=(airflow_username, airflow_password)
                                 )

        if response.status_code == 200:
            logging.info('Connection created successfully!')
        else:
            raise HTTPException(status_code=response.status_code,
                                detail=response.text
                                )
"""
    def add_task(self, task):
        # self.tasks.append(task)
        pass
