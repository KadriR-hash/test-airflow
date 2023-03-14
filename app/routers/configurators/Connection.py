import logging
import os
from abc import ABC
import requests
import json
from dotenv import load_dotenv
from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException
from .DBConnection import DBConnection
from .models.DBConfigurator import DBConfigurator
logging.basicConfig(level=logging.INFO)


class Connection(DBConnection):
    """__instance = None"""

    """def __init__(self):
        if Connection.__instance is not None:
            raise Exception("Cannot create multiple instances of PostgresqlConnection class.")
        else:
            Connection.__instance = self
            self.connections = {}
    """

    def connect_to_db(self, db_configurator: DBConfigurator):
        try:
            load_dotenv()
            airflow_api_url = os.environ.get('AIRFLOW_API_URL')
            airflow_username = os.environ.get('AIRFLOW_USERNAME')
            airflow_password = os.environ.get('AIRFLOW_PASSWORD')
            payload = jsonable_encoder(db_configurator)
            # Send the POST request to create the connection
            response = requests.post(
                f'{airflow_api_url}/connections',
                json=payload,
                auth=(airflow_username, airflow_password)
            )

            if response.status_code == 200:
                logging.info('Connection created successfully!')
            else:
                raise HTTPException(status_code=response.status_code,
                                    detail=response.text
                                    )

            logging.info(f"Successfully connected to {db_configurator.connection_id} database.")
        except Exception as e:
            logging.info(f"Could not connect to the database: {e}")

    def close(self, my_conn_id):
        try:
            load_dotenv()
            airflow_api_url = os.environ.get('AIRFLOW_API_URL')
            airflow_username = os.environ.get('AIRFLOW_USERNAME')
            airflow_password = os.environ.get('AIRFLOW_PASSWORD')
            # Send the DELETE request to delete the connection
            response = requests.delete(
                f'{airflow_api_url}/connections/{my_conn_id}',
                auth=(airflow_username, airflow_password)
            )

            if response.status_code == 200:
                logging.info('Connection deleted successfully!')
            else:
                raise HTTPException(status_code=response.status_code,
                                    detail=response.text
                                    )
            logging.info(f"Successfully closed connection to {my_conn_id} database.")
        except Exception as e:
            logging.info(f"Could not close the database connection: {e}")

    """@staticmethod
    def get_instance():
        if Connection.__instance is None:
            Connection()
        return Connection.__instance"""
