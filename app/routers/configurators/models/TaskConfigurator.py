from typing import Dict

from pydantic import BaseModel


class TaskConfigurator(BaseModel):
    task_id: str
    operator: str
    my_params: Dict
    dependencies: list[str]
