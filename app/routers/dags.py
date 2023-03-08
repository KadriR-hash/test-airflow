import requests
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from .configurators.DAG import DAG
from .configurators.models.SimpleDAGConfigurator import SimpleDAGConfigurator
import yaml

router = APIRouter()


@router.post("/dags", response_model=SimpleDAGConfigurator)
async def create_dag(dag_configurator: SimpleDAGConfigurator):
    # dag = DAG()
    # dag.create_dag(dag_configurator)
    payload = jsonable_encoder(dag_configurator)
    with open('/outputs/output.yaml', 'w+') as file:
        yaml.dump(payload, file)

    return dag_configurator
