import logging
from abc import ABC
from .DAGConfigurator import DAGConfigurator
from .Task import Task
from .models.SimpleDAGConfigurator import SimpleDAGConfigurator

logging.basicConfig(level=logging.INFO)


class DAG(DAGConfigurator, ABC):

    def create_dag(self, dag: SimpleDAGConfigurator):
        dag_dict = dag.dict()
        dag_dict_copy = dag_dict.copy()
        for key in dag_dict_copy.keys():
            if key == 'dag_id':
                element = dag_dict_copy[key]
                dag_dict.pop(key)
                new_dict = {element: dag_dict}
            if key == 'extra_args':
                dag_dict.pop(key)
                dag_dict.update(dag_dict_copy[key])
        for key in new_dict[element].keys():
            if key == 'tasks':
                tasks = {}
                for item in new_dict[element][key]:
                    task = Task()
                    task_dict, element = task.create_task(item)
                    tasks[element] = task_dict
                dag_dict['tasks'] = tasks
                logging.info(f"tasks:{tasks}")
        return new_dict

    def add_task(self, task):
        # self.tasks.append(task)
        pass
