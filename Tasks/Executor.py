import logging

import duckdb
import pandas as pd
from beartype import beartype

from ConnectionPool import connection_pool as cp
from package_utils.design_patterns import Singleton
from .TaskBase import Task

cp = cp.conns


class TaskExecutor(metaclass=Singleton):
    """
    Class that executes a task. Should have only one instance.
    """

    @beartype
    def __init__(self, task: (Task, None) = None, **options):
        self.options    = options
        self.task       = task
        self.final_df   = pd.DataFrame()

    @beartype
    def _register_data(self, data: pd.DataFrame, data_type='raw_query'):
        """
        Register the data into the duckdb database to execute 'stage queries' on them.
        :param data: Data that you want to be registered in duck db
        :param data_type: default raw_query other option -> 'stage_query',
        """
        table_name = self.task.raw_alias if data_type == 'raw_query' else self.task.stage_alias
        # register the table in the globals variable so that when we run duck db query,
        # we are able to find that reference.
        logging.info(f"Registering table name : {table_name} for top 1 row of data :\n{data.head(1)}")
        globals()[table_name] = data

    def execute_query(self, query_type='source'):
        """
        :param query_type: default 'source'. Will execute the raw_query on source_connection_name
        another option can be 'stage' which will execute query on duck db
        """
        if query_type == 'source':
            if self.task.raw_query or self.task.raw_query == {}:
                logging.info(f"Executing the source raw query")
                data_df = cp[self.task.source_connection_name].execute_raw_query(
                    self.task.raw_query,
                    **{**self.task.options, **self.task.optional_param}
                )
                # set the final_df which will be stored in the target connection
                logging.info(f"Data count : {len(data_df)} and columns : {list(data_df.columns)}")
                self.final_df = data_df
                # now register the data in the duck db
                self._register_data(data_df)
        elif query_type == 'stage':
            if self.task.stage_query:
                logging.info(f"Executing the stage query on the duck db : {self.task.stage_query}")
                self.final_df = duckdb.query(self.task.stage_query).to_df()
                logging.info(f"Data count : {len(self.final_df)} and columns : {list(self.final_df.columns)}")
                self._register_data(self.final_df, 'stage_query')
        else:
            raise ValueError(f"Wrong query_type parameter : {query_type}")

    def save_final_result(self):
        """
        Save whatever is the final result to the target location
        """
        logging.info(f"Logging the final result")
        if not isinstance(self.task.target_connection_name, list):
            self.task.target_connection_name = [self.task.target_connection_name]
        for conn_name in self.task.target_connection_name:
            logging.info(f"Saving the data in connection name : {conn_name}")
            cp[conn_name].persist(
                self.final_df,
                to_container=self.task.target_container_name,
                if_exists=self.task.save_mode or "replace",
                **{**self.task.options, **self.task.optional_param}
            )
        logging.info(f"Successfully saved the data into the final location. Execution of the task completed.")

    def execute_task(self):
        """
        Call this to execute the whole flow of the task, decided by this class, else create your own flow
        """
        
        logging.info(f"{'*' * 20} Task Execution Started {'*' * 20}")
        logging.info(f"Executing the task : \n {self.task}")
        # first assert that the source_connection_name and target_connection_name mentioned in the task
        # are present in the connection pool or not.
        assert self.task.source_connection_name in cp, \
            f"Source connection name is not present in the connection pool : " \
            f"{cp.conns}"
        # assert there should be a task here
        assert self.task, "self.task is empty yaar. How can it work?"
        # let's execute the raw_query first
        logging.info('Executing the raw query')
        self.execute_query()
        logging.info('Executing stage query if there is any')
        self.execute_query('stage')
        logging.info('Now the last step, storing the data')
        self.save_final_result()
        logging.info(f"{'*' * 20} Task Execution Completed {'*' * 20}")

    def set_task(self, task: Task):
        """
        Setter for task
        """
        self.task = task
    
    @beartype
    def execute_multiple_tasks(self, tasks: list[Task]):
        """ 
        If there are multiple tasks, this will execute it
        """
        for task in tasks:
            self.set_task(task)
            self.execute_task()