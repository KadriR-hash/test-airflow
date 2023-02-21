import logging

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from operators.exceptions.TableCreationException import TableCreationException
from sqlalchemy_utils.types.pg_composite import psycopg2
from airflow.utils.state import State


class CreateTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, sql, *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, context):
        try:
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            result = cur.execute(self.sql)
            logging.info(f"number of rows affected : {result}")
            conn.commit()
            cur.close()
            conn.close()

        except psycopg2.OperationalError:
            raise TableCreationException("Failed to connect to database")
        except psycopg2.errors.DuplicateTable:
            logging.info("Table already exist")
            ti = context.get('ti')
            # ti.set_state(State.SKIPPED)
            logging.info(f'State : {ti.state}')
            raise AirflowSkipException
        except Exception:
            raise TableCreationException("Not identified exception")
