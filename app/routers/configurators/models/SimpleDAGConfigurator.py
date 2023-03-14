from pydantic import BaseModel
from typing import Dict

from .TaskConfigurator import TaskConfigurator


class SimpleDAGConfigurator(BaseModel):
    dag_id: str
    default_args: Dict
    extra_args: Dict
    tasks: list[TaskConfigurator]
