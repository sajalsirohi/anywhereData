from GlobalBaseClasses import Task
from package_utils.design_patterns import Singleton


class TaskCreator(Task):
    """
    Creates the task using the config that is passed
    """
    def __init__(self, config, **options):
        super().__init__(config, **options)


class TaskPool(metaclass=Singleton):
    """
    Here we are going to store all the tasks based on the task_id
    """
    def __init__(self, tasks_config = None, **options):
