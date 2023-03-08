"""import requests
from fastapi import APIRouter, HTTPException

from .configurators.DAG import DAG
from .configurators.models.SimpleDAGConfigurator import SimpleDAGConfigurator
from .configurators.models.TaskConfigurator import TaskConfigurator

router = APIRouter()

# in-memory "db"
tasks = {}


@router.post("/tasks", response_model=TaskConfigurator)
async def create_task(task: TaskConfigurator):
    if task.task_id in tasks:
        raise HTTPException(status_code=400, detail="Task already exists")

    tasks[task.task_id] = task
    return task


@router.get("/tasks")
async def get_tasks():
    return tasks
"""