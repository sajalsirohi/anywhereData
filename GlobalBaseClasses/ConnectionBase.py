import logging
from abc import ABC, abstractmethod

import pandas as pd
from beartype import beartype

from package_utils import read_file


class Config:
    __doc__ = """
    Base class for config, which is passed onto to connection factory, to establish a 
    connection.
    """
    # connection type info which is passed into every class that implements this class
    connection_type = None

    def __init__(self, config, **options):
        logging.info(f"Parsing the config : {config}")
        self.config           = config  # connections.yaml
        self.options          = options
        self.host             = self.config.get('host')
        self.password         = self.config.get('password')
        self.username         = self.config.get('username')
        self.db_name          = self.config.get('db_name')
        self.port             = self.config.get('port')
        self.connection_str   = self.config.get('connection_str')

        if self.password == '':
            if 'password_path' in self.config:
                self.password = read_file(self.config.get('password_path'))
            else:
                self.password = ''
            logging.info(f"Password successfully fetched, will be connecting using the credentials")

    def __str__(self):
        return f"""
        HOST NAME   : {self.host}
        PASSWORD    : {len(self.password) * "*"}
        USERNAME    : {self.username}
        DB NAME     : {self.db_name}
        PORT        : {self.port}
        """


class Connection(ABC):
    __doc__ = """
    A connection type abstract class. 
    """
    connection_name = None

    @abstractmethod
    def execute_raw_query(self, query, **options):
        pass

    @abstractmethod
    @beartype
    def persist(self, data: pd.DataFrame, to_container, **options):
        pass

    @abstractmethod
    def set_connection(self):
        pass