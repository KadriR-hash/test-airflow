from pydantic import BaseModel
from typing import Optional

from .TaskConfigurator import TaskConfigurator


class SimpleDAGConfigurator(BaseModel):
    dag_id: str
    owner: str
    schedule_interval: Optional[str]
    start_date: Optional[str]
    dagrun_timeout: Optional[str]
    tasks: list[TaskConfigurator] = []
