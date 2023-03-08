from typing import Dict

from pydantic import BaseModel

from .Operators import Operators


class TaskConfigurator(BaseModel):
    task_id: str
    operator: str
    my_params: Dict
    upstreams: list[str] = []
    downstreams: list[str] = []
