import logging
from fastapi import APIRouter

from .configurators.models.DBConfigurator import DBConfigurator
from .configurators.Connection import Connection

logging.basicConfig(level=logging.INFO)
router = APIRouter()


@router.post("/connections", response_model=DBConfigurator)
async def create_connection(db_configurator: DBConfigurator):
    db_connection = Connection()
    db_connection.connect_to_db(db_configurator)
    return db_configurator


@router.delete("/connections/{my_conn_id}")
async def create_connection(my_conn_id: str):
    db_connection = Connection()
    db_connection.close(my_conn_id)
    return my_conn_id
