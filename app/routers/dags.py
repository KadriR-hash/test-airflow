import logging

import requests
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from .configurators.DAG import DAG
from .configurators.Task import Task
from .configurators.models.SimpleDAGConfigurator import SimpleDAGConfigurator
import yaml

router = APIRouter()


@router.post("/dags", response_model=SimpleDAGConfigurator)
async def create_dag(dag_configurator: SimpleDAGConfigurator):
    dag = DAG()
    dag_dict = dag.create_dag(dag_configurator)
    data = jsonable_encoder(dag_dict)
    payload = yaml.dump(data, sort_keys=False)
    with open('/outputs/output.yaml', 'w+') as file:
        file.write(payload)

    return dag_configurator
