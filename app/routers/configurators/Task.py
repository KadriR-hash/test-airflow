import logging

from .models.TaskConfigurator import TaskConfigurator

logging.basicConfig(level=logging.INFO)


class Task:
    def create_task(self, task_dict: dict):
        task_dict_copy = task_dict.copy()
        for key in task_dict_copy.keys():
            if key == 'task_id':
                element = task_dict_copy[key]
                task_dict.pop(key)

            if key == 'my_params':
                task_dict.pop(key)
                task_dict.update(task_dict_copy[key])

        return task_dict, element
