import logging

import networkx as nx
from beartype import beartype

from package_utils.design_patterns import Singleton
from Tasks.Creator import TaskPool
from Tasks.Executor import TaskExecutor

tp = TaskPool()
te = TaskExecutor()

tp.create_task_pool()


class Pipeline(metaclass=Singleton):
    """
    Main class that creates and executes the pipeline
    """
    @beartype
    def __init__(self, pipeline_config: dict = None, **options):
        if pipeline_config is None:
            from package_utils import pipeline_config as pc
            self.pipeline_config = pc
        else:
            self.pipeline_config = pipeline_config
        self.options = options
        self.pipelines = {}
        logging.info(f"This is the pipeline I have : {self.pipeline_config}")
        self._parse_pipelines()

    def execute_pipelines(self):
        """
        Execute the pipelines that we have
        """
        for pipe_name, tasks_graph in self.pipelines.items():
            logging.info(f"Executing the pipe name : {pipe_name} and tasks : {tasks_graph.nodes()}")
            te.execute_multiple_tasks(list(tasks_graph.nodes()))
            logging.info("$$$$$$$$$$$$$$$$$$$ Pipeline Completed $$$$$$$$$$$$$$$$$$$")

    def _parse_pipelines(self):
        """
        Parse the pipelines that are in string format
        """
        for pipeline_name, pipeline_str in self.pipeline_config.items():
            task_graph = nx.DiGraph()
            logging.info(f"Parsing the config : {pipeline_name}")
            # replace the sep with the true sep my brother
            pipeline_str = pipeline_str.replace(":", "->").replace(",", "->").replace("~", "->")
            # splitting it. it is a task list basically and now let's create a cyclic graph of it
            tasks = [val.strip() for val in pipeline_str.split("->")]
            # tasks should have length of greater than 2 my brother

            for task in tasks:
                # adding the nodes
                logging.info(f"Fetching the task : {task} from task pool")
                task_graph.add_node(tp.pool[task])
            if len(tasks) >= 2:
                for itr in range(len(tasks) - 1):
                    logging.info(f"Creating an edge between : {tasks[itr]} -> {tasks[itr+1]}")
                    task_graph.add_edge(tp.pool[tasks[itr]], tp.pool[tasks[itr+1]])

            # add the pipelines into the pipeline_name
            self.pipelines[pipeline_name] = task_graph