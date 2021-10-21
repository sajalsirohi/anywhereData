import logging

from package_utils.design_patterns import Singleton
from .TaskBase import Task


class TaskPool(metaclass=Singleton):
    """
    Here we are going to store all the tasks based on the task_id
    """
    def __init__(self, tasks_config=None, **options):
        from package_utils import tasks_config as tc
        self.tasks_config = tc or tasks_config
        if isinstance(self.tasks_config, dict):
            self.tasks_config = [self.tasks_config]
        self.options = options
        self.pool = {}

    def create_task_pool(self):
        """
        Create the task pool
        """
        logging.info(f"Creating the tasks pool")
        for task in self.tasks_config:
            logging.info(f"Creating the task for task_id : {task['id']}")
            self.pool[task['id']] = Task(task)
        logging.info(f"Total task ids present in the pool : {list(self.pool.keys())}")