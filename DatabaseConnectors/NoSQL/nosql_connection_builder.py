import logging
from abc import ABC, abstractmethod

from beartype import beartype

from GlobalBaseClasses import Connection
from .MongoDB.mongo_config_builder import MongoConfig


class NoSQLConnection(Connection, ABC):
    """
    NoSQL connection object
    """
    @beartype
    def __init__(self,
                 config: MongoConfig,
                 connection_name,
                 **options):
        logging.info(f"Connecting to NoSQL server with config : \n {config}")
        self.config          = config
        self.current_df      = None
        self.connection_name = connection_name
        self.options         = options
        self.jdbc_url        = 'Not as of now'

    @beartype
    def get_data(self, query: dict, **options):
        """
        Execute the query
        :param query:
        :return:
        """
        logging.info(f"Executing on connection_name -> :'{self.connection_name}': in NoSQL")
        logging.info(f"Executing the query : {query}")
        try:
            return self.execute_nosql_query(query, **options)
        except Exception as err:
            logging.error(f"Error occurred while executing query : {query}, Error stack - {err}")
            if options.get('ignore_errors'):
                return ""
            else:
                raise err

    def send_data(self, data, to_container, **options):
        """
        Put the values of df to `to_table`
        :param data:
        :param to_container:
        :return:
        """
        logging.info(f'Processing the data to the table {to_container}')
        try:
            self.persist_nosql(data, to_container, **options)
        except Exception as err:
            logging.error(f"Error occurred while storing data to {to_container}\n Error stack - {err}")
            raise err

    @abstractmethod
    def execute_nosql_query(self, query, **options):
        """
        An abstract method that will be overwritten by the derived class
        """
        pass

    @abstractmethod
    def persist_nosql(self, data, to_container, **options):
        """
        An abstract method that will be overwritten by the derived class
        """
        pass